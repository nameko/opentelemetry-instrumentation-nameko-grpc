# -*- coding: utf-8 -*-
import time
from unittest.mock import Mock, patch

import grpc
import nameko_grpc.errors
import pytest
from nameko.testing.services import entrypoint_waiter
from nameko.testing.utils import get_extension
from nameko_grpc.client import Client
from nameko_grpc.dependency_provider import GrpcProxy
from nameko_grpc.entrypoint import Grpc
from nameko_grpc.errors import GRPC_DETAILS_METADATA_KEY, GrpcError, make_status
from nameko_opentelemetry import active_tracer
from opentelemetry import trace
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode


class TestCardinalities:
    @pytest.fixture
    def container(self, protos, services, container_factory):

        grpc = Grpc.implementing(services.exampleStub)

        class ExampleService:
            name = "example"

            @grpc
            def unary_unary(self, request, context):
                message = request.value * (request.multiplier or 1)
                return protos.ExampleReply(message=message)

            @grpc
            def unary_stream(self, request, context):
                message = request.value * (request.multiplier or 1)
                for i in range(request.response_count):
                    yield protos.ExampleReply(message=message, seqno=i + 1)

            @grpc
            def stream_unary(self, request, context):
                messages = []
                for index, req in enumerate(request):
                    message = req.value * (req.multiplier or 1)
                    messages.append(message)

                return protos.ExampleReply(message=",".join(messages))

            @grpc
            def stream_stream(self, request, context):
                for index, req in enumerate(request):
                    message = req.value * (req.multiplier or 1)
                    yield protos.ExampleReply(message=message, seqno=index + 1)

        container = container_factory(ExampleService)
        container.start()

        yield container

        container.stop()

    @pytest.fixture
    def client(self, grpc_port, container, services):
        with Client(
            "//localhost:{}".format(grpc_port), services.exampleStub,
        ) as client:
            yield client

    def test_unary_unary(self, container, client, protos, memory_exporter):
        with entrypoint_waiter(container, "unary_unary"):
            response = client.unary_unary(protos.ExampleRequest(value="A"))
            assert response.message == "A"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        for span in spans:
            assert span.attributes["rpc.grpc.cardinality"] == "UNARY_UNARY"

    def test_unary_stream(self, container, client, protos, memory_exporter):
        with entrypoint_waiter(container, "unary_stream"):
            responses = client.unary_stream(
                protos.ExampleRequest(value="A", response_count=2)
            )
            assert [(response.message, response.seqno) for response in responses] == [
                ("A", 1),
                ("A", 2),
            ]

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        for span in spans:
            assert span.attributes["rpc.grpc.cardinality"] == "UNARY_STREAM"

    def test_stream_unary(self, container, client, protos, memory_exporter):
        def generate_requests():
            for value in ["A", "B"]:
                yield protos.ExampleRequest(value=value)

        with entrypoint_waiter(container, "stream_unary"):
            response = client.stream_unary(generate_requests())
            assert response.message == "A,B"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        for span in spans:
            assert span.attributes["rpc.grpc.cardinality"] == "STREAM_UNARY"

    def test_stream_stream(self, container, client, protos, memory_exporter):
        def generate_requests():
            for value in ["A", "B"]:
                yield protos.ExampleRequest(value=value)

        with entrypoint_waiter(container, "stream_stream"):
            responses = client.stream_stream(generate_requests())
            assert [(response.message, response.seqno) for response in responses] == [
                ("A", 1),
                ("B", 2),
            ]

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        for span in spans:
            assert span.attributes["rpc.grpc.cardinality"] == "STREAM_STREAM"


class TestCaptureIncomingContext:
    @pytest.fixture
    def container(self, protos, grpc_port, services, container_factory):

        grpc = Grpc.implementing(services.exampleStub)

        class ExampleService:
            name = "example"

            self_grpc = GrpcProxy(
                "//localhost:{}".format(grpc_port), services.exampleStub
            )

            @grpc
            def stream_stream(self, request, context):
                for index, req in enumerate(request):
                    message = req.value * (req.multiplier or 1)
                    yield protos.ExampleReply(message=message, seqno=index + 1)

        container = container_factory(ExampleService)
        container.start()

        yield container

        container.stop()

    @pytest.fixture(params=["standalone", "dependency_provider"])
    def client(self, grpc_port, services, request, container):
        if request.param == "standalone":
            with Client(
                "//localhost:{}".format(grpc_port), services.exampleStub,
            ) as client:
                yield client
        if request.param == "dependency_provider":
            dp = get_extension(container, GrpcProxy)
            yield dp.get_dependency(Mock(context_data={}))

    def test_incoming_context(self, container, client, protos, memory_exporter):
        def generate_requests():
            for value in ["A", "B"]:
                yield protos.ExampleRequest(value=value)

        with entrypoint_waiter(container, "stream_stream"):
            responses = client.stream_stream(generate_requests())
            assert [(response.message, response.seqno) for response in responses] == [
                ("A", 1),
                ("B", 2),
            ]

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.CLIENT, spans))[0]
        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert client_span.parent is None
        assert server_span.parent.span_id == client_span.get_span_context().span_id


class TestNoEntrypointFired:
    @pytest.fixture
    def container(self, protos, services, container_factory):

        grpc = Grpc.implementing(services.exampleStub)

        class ExampleService:
            name = "example"

            @grpc
            def unary_unary(self, request, context):  # pragma: no cover
                message = request.value * (request.multiplier or 1)
                return protos.ExampleReply(message=message)

        container = container_factory(ExampleService)
        container.start()

        yield container

        container.stop()

    @pytest.fixture
    def client(self, grpc_port, container, services):
        with Client(
            "//localhost:{}".format(grpc_port), services.exampleStub,
        ) as client:
            yield client

    def test_client_method_not_found(self, protos, container, client, memory_exporter):

        with pytest.raises(GrpcError) as error:
            client.not_found(protos.ExampleRequest(value="hello"))
        assert error.value.code == grpc.StatusCode.UNIMPLEMENTED
        assert error.value.message == "Method not found!"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert not server_span.status.is_ok
        assert server_span.status.status_code == StatusCode.ERROR
        assert server_span.status.description == "Method not found!"


class TestServerAttributes:
    @pytest.fixture
    def container(self, protos, services, container_factory):

        grpc = Grpc.implementing(services.exampleStub)

        class ExampleService:
            name = "example"

            @grpc
            def unary_unary(self, request, context):
                message = request.value * (request.multiplier or 1)
                return protos.ExampleReply(message=message)

        container = container_factory(ExampleService)
        container.start()

        yield container

        container.stop()

    @pytest.fixture
    def client(self, grpc_port, container, services):
        with Client(
            "//localhost:{}".format(grpc_port), services.exampleStub,
        ) as client:
            yield client

    def test_attributes(self, container, client, protos, memory_exporter):

        with entrypoint_waiter(container, "unary_unary"):
            response = client.unary_unary(protos.ExampleRequest(value="A"))
            assert response.message == "A"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        attributes = server_span.attributes
        assert attributes["rpc.system"] == "grpc"
        assert attributes["rpc.method"] == "unary_unary"
        assert attributes["rpc.service"] == "nameko.example"  # full package name
        assert attributes["rpc.grpc.cardinality"] == "UNARY_UNARY"


class TestClientAttributes:
    @pytest.fixture
    def container(self, grpc_port, protos, services, container_factory):

        grpc = Grpc.implementing(services.exampleStub)

        class ExampleService:
            name = "example"

            self_grpc = GrpcProxy(
                "//localhost:{}".format(grpc_port), services.exampleStub
            )

            @grpc
            def unary_unary(self, request, context):
                message = request.value * (request.multiplier or 1)
                return protos.ExampleReply(message=message)

        container = container_factory(ExampleService)
        container.start()

        yield container

        container.stop()

    @pytest.fixture(params=["standalone", "dependency_provider"])
    def client(self, grpc_port, services, request, container):
        if request.param == "standalone":
            with Client(
                "//localhost:{}".format(grpc_port), services.exampleStub,
            ) as client:
                yield client
        if request.param == "dependency_provider":
            dp = get_extension(container, GrpcProxy)
            yield dp.get_dependency(Mock(context_data={}))

    def test_attributes(self, container, client, protos, memory_exporter):

        with entrypoint_waiter(container, "unary_unary"):
            response = client.unary_unary(protos.ExampleRequest(value="A"))
            assert response.message == "A"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.CLIENT, spans))[0]

        attributes = client_span.attributes
        assert attributes["rpc.system"] == "grpc"
        assert attributes["rpc.method"] == "unary_unary"
        assert attributes["rpc.service"] == "nameko.example"  # full package name
        assert attributes["rpc.grpc.cardinality"] == "UNARY_UNARY"


class TestAdditionalSpans:
    @pytest.fixture
    def container(self, protos, services, container_factory):

        grpc = Grpc.implementing(services.exampleStub)

        class ExampleService:
            name = "example"

            @grpc
            def unary_unary(self, request, context):
                with active_tracer().start_as_current_span(
                    "foobar", attributes={"foo": "bar"}
                ):
                    message = request.value * (request.multiplier or 1)
                    return protos.ExampleReply(message=message)

        container = container_factory(ExampleService)
        container.start()

        yield container

        container.stop()

    @pytest.fixture
    def client(self, grpc_port, container, services):
        with Client(
            "//localhost:{}".format(grpc_port), services.exampleStub,
        ) as client:
            yield client

    def test_internal_span(self, container, client, protos, memory_exporter):

        with entrypoint_waiter(container, "unary_unary"):
            response = client.unary_unary(protos.ExampleRequest(value="A"))
            assert response.message == "A"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 3

        internal_span = list(
            filter(lambda span: span.kind == SpanKind.INTERNAL, spans)
        )[0]

        assert internal_span.name == "foobar"


class TestCallArgsAttributes:
    @pytest.fixture(
        params=[True, False], ids=["send_request_payloads", "no_request_payloads"]
    )
    def send_request_payloads(self, request):
        return request.param

    @pytest.fixture
    def config(self, config, send_request_payloads):
        # disable request payloads based on param
        config["send_request_payloads"] = send_request_payloads
        return config

    @pytest.fixture
    def container(self, protos, services, container_factory):

        grpc = Grpc.implementing(services.exampleStub)

        class ExampleService:
            name = "example"

            @grpc
            def unary_unary(self, request, context):
                message = request.value * (request.multiplier or 1)
                return protos.ExampleReply(message=message)

            @grpc
            def stream_unary(self, request, context):
                messages = []
                for index, req in enumerate(request):
                    message = req.value * (req.multiplier or 1)
                    messages.append(message)

                return protos.ExampleReply(message=",".join(messages))

            @grpc
            def unary_stream(self, xrequest, context):
                message = xrequest.value * (xrequest.multiplier or 1)
                for i in range(xrequest.response_count):
                    yield protos.ExampleReply(message=message, seqno=i + 1)

        container = container_factory(ExampleService)
        container.start()

        yield container

        container.stop()

    @pytest.fixture
    def client(self, grpc_port, container, services):
        with Client(
            "//localhost:{}".format(grpc_port), services.exampleStub,
        ) as client:
            yield client

    def test_unary_request(
        self, protos, client, container, memory_exporter, send_request_payloads
    ):
        with entrypoint_waiter(container, "unary_unary"):
            response = client.unary_unary(protos.ExampleRequest(value="A"))
            assert response.message == "A"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        attributes = server_span.attributes

        if send_request_payloads:
            assert attributes["rpc.grpc.request"] == "{'value': 'A'}"
        else:
            assert "rpc.grpc.request" not in attributes

    def test_streaming_request(
        self, protos, client, container, memory_exporter, send_request_payloads
    ):
        def generate_requests():
            for value in ["A", "B"]:
                yield protos.ExampleRequest(value=value)

        with entrypoint_waiter(container, "stream_unary"):
            response = client.stream_unary(generate_requests())
            assert response.message == "A,B"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        attributes = server_span.attributes

        if send_request_payloads:
            assert attributes["rpc.grpc.request"] == "{'value': 'A'} | {'value': 'B'}"
        else:
            assert "rpc.grpc.request" not in attributes

    def test_different_argument_name(
        self, protos, client, container, memory_exporter, send_request_payloads
    ):
        with entrypoint_waiter(container, "unary_stream"):
            responses = client.unary_stream(
                protos.ExampleRequest(value="A", response_count=2)
            )
            assert [(response.message, response.seqno) for response in responses] == [
                ("A", 1),
                ("A", 2),
            ]

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        attributes = server_span.attributes
        if send_request_payloads:
            assert (
                attributes["rpc.grpc.request"] == "{'value': 'A', 'responseCount': 2}"
            )
        else:
            assert "rpc.grpc.request" not in attributes


class TestResultAttributes:
    @pytest.fixture(
        params=[True, False], ids=["send_response_payloads", "no_response_payloads"]
    )
    def send_response_payloads(self, request):
        return request.param

    @pytest.fixture
    def config(self, config, send_response_payloads):
        # disable request payloads based on param
        config["send_response_payloads"] = send_response_payloads
        return config

    @pytest.fixture
    def container(self, protos, services, container_factory):

        grpc = Grpc.implementing(services.exampleStub)

        class ExampleService:
            name = "example"

            @grpc
            def unary_unary(self, request, context):
                message = request.value * (request.multiplier or 1)
                return protos.ExampleReply(message=message)

            @grpc
            def unary_stream(self, request, context):
                message = request.value * (request.multiplier or 1)
                for i in range(request.response_count):
                    yield protos.ExampleReply(message=message, seqno=i + 1)

        container = container_factory(ExampleService)
        container.start()

        yield container

        container.stop()

    @pytest.fixture
    def client(self, grpc_port, container, services):
        with Client(
            "//localhost:{}".format(grpc_port), services.exampleStub,
        ) as client:
            yield client

    def test_unary_response(
        self, container, client, protos, memory_exporter, send_response_payloads
    ):
        with entrypoint_waiter(container, "unary_unary"):
            response = client.unary_unary(protos.ExampleRequest(value="A"))
            assert response.message == "A"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        attributes = server_span.attributes
        if send_response_payloads:
            assert attributes["rpc.grpc.response"] == "{'message': 'A'}"
        else:
            assert "rpc.grpc.response" not in attributes

    def test_stream_response(
        self, container, client, protos, memory_exporter, send_response_payloads
    ):
        with entrypoint_waiter(container, "unary_stream"):
            responses = client.unary_stream(
                protos.ExampleRequest(value="A", response_count=2)
            )
            assert [(response.message, response.seqno) for response in responses] == [
                ("A", 1),
                ("A", 2),
            ]

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        attributes = server_span.attributes

        if send_response_payloads:
            assert (
                attributes["rpc.grpc.response"]
                == "{'message': 'A', 'seqno': 1} | {'message': 'A', 'seqno': 2}"
            )
        else:
            assert "rpc.grpc.response" not in attributes


class TestNoTracer:
    @pytest.fixture
    def container(self, protos, services, container_factory):

        grpc = Grpc.implementing(services.exampleStub)

        class ExampleService:
            name = "example"

            @grpc
            def unary_unary(self, request, context):
                message = request.value * (request.multiplier or 1)
                return protos.ExampleReply(message=message)

        container = container_factory(ExampleService)
        container.start()

        yield container

        container.stop()

    @pytest.fixture
    def client(self, grpc_port, container, services):
        with Client(
            "//localhost:{}".format(grpc_port), services.exampleStub,
        ) as client:
            yield client

    @pytest.fixture
    def trace_provider(self):
        """ Temporarily replace the configured trace provider with the default
        provider that would be used if no SDK was in use.
        """
        with patch("nameko_opentelemetry.trace") as patched:
            patched.get_tracer.return_value = trace.get_tracer(
                __name__, "", trace._DefaultTracerProvider()
            )
            yield

    def test_not_recording(
        self, trace_provider, protos, client, container, memory_exporter
    ):
        with entrypoint_waiter(container, "unary_unary"):
            response = client.unary_unary(protos.ExampleRequest(value="A"))
            assert response.message == "A"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 0


class TestExceptions:
    @pytest.fixture
    def container(self, container_factory, services, protos):

        grpc = Grpc.implementing(services.exampleStub)

        class Error(Exception):
            pass

        class Service:
            name = "service"

            @grpc
            def unary_unary(self, request, context):
                message = request.value * (request.multiplier or 1)
                return protos.ExampleReply(message=message)

            @grpc
            def unary_error(self, request, context):
                raise Error("boom")

            @grpc
            def unary_error_via_context(self, request, context):
                code = nameko_grpc.errors.StatusCode.UNAUTHENTICATED
                message = "Not allowed!"

                context.set_code(code)
                context.set_message(message)
                context.set_trailing_metadata(
                    [
                        (
                            GRPC_DETAILS_METADATA_KEY,
                            make_status(code, message).SerializeToString(),
                        )
                    ]
                )

            @grpc
            def unary_grpc_error(self, request, context):
                code = nameko_grpc.errors.StatusCode.UNAUTHENTICATED
                message = "Not allowed!"

                raise GrpcError(
                    code=code, message=message, status=make_status(code, message)
                )

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture
    def client(self, grpc_port, container, services):
        with Client(
            "//localhost:{}".format(grpc_port), services.exampleStub,
        ) as client:
            yield client

    def test_success(self, protos, client, container, memory_exporter):
        with entrypoint_waiter(container, "unary_unary"):
            response = client.unary_unary(protos.ExampleRequest(value="A"))
            assert response.message == "A"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        # no exception
        assert len(server_span.events) == 0

    def test_raise_exception(self, protos, client, container, memory_exporter):
        with entrypoint_waiter(container, "unary_error"):
            with pytest.raises(GrpcError):
                client.unary_error(protos.ExampleRequest(value="A"))

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert len(server_span.events) == 1
        event = server_span.events[0]

        assert event.name == "exception"
        assert event.attributes["exception.type"] == "Error"
        assert event.attributes["exception.message"] == "boom"

    def test_raise_grpc_error(self, protos, client, container, memory_exporter):
        with entrypoint_waiter(container, "unary_grpc_error"):
            with pytest.raises(GrpcError):
                client.unary_grpc_error(protos.ExampleRequest(value="A"))

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert len(server_span.events) == 1
        event = server_span.events[0]

        assert event.name == "exception"
        assert event.attributes["exception.type"] == "GrpcError"
        assert event.attributes["exception.message"] == "Not allowed!"

    def test_error_via_context(self, protos, client, container, memory_exporter):
        with entrypoint_waiter(container, "unary_error_via_context"):
            with pytest.raises(GrpcError):
                client.unary_error_via_context(protos.ExampleRequest(value="A"))

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        # no exception
        assert len(server_span.events) == 0


class TestPartialSpanInClient:
    @pytest.fixture
    def container(self, grpc_port, protos, services, container_factory):

        grpc = Grpc.implementing(services.exampleStub)

        class ExampleService:
            name = "example"

            self_grpc = GrpcProxy(
                "//localhost:{}".format(grpc_port), services.exampleStub
            )

            @grpc
            def unary_unary(self, request, context):
                message = request.value * (request.multiplier or 1)
                return protos.ExampleReply(message=message)

        container = container_factory(ExampleService)
        container.start()

        yield container

        container.stop()

    @pytest.fixture(params=["standalone", "dependency_provider"])
    def client(self, grpc_port, services, request, container):
        if request.param == "standalone":
            with Client(
                "//localhost:{}".format(grpc_port), services.exampleStub,
            ) as client:
                yield client
        if request.param == "dependency_provider":
            dp = get_extension(container, GrpcProxy)
            yield dp.get_dependency(Mock(context_data={}))

    @patch("nameko_grpc_opentelemetry.active_spans")
    def test_span_not_started(
        self, active_spans, protos, client, container, memory_exporter
    ):

        # fake a missing span
        active_spans.get.return_value = None

        with pytest.warns(UserWarning) as warnings:
            with entrypoint_waiter(container, "unary_unary"):
                response = client.unary_unary(protos.ExampleRequest(value="A"))
                assert response.message == "A"

        assert "no active span" in str(warnings[0].message)

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        # server span only
        assert spans[0].kind == SpanKind.SERVER


class TestClientStatus:
    @pytest.fixture
    def container(self, grpc_port, protos, services, container_factory):

        grpc = Grpc.implementing(services.exampleStub)

        class ExampleService:
            name = "example"

            self_grpc = GrpcProxy(
                "//localhost:{}".format(grpc_port), services.exampleStub
            )

            @grpc
            def unary_unary(self, request, context):
                message = request.value * (request.multiplier or 1)
                time.sleep(request.delay / 10)
                return protos.ExampleReply(message=message)

        container = container_factory(ExampleService)
        container.start()

        yield container

        container.stop()

    @pytest.fixture(params=["standalone", "dependency_provider"])
    def client(self, grpc_port, services, request, container):
        if request.param == "standalone":
            with Client(
                "//localhost:{}".format(grpc_port), services.exampleStub,
            ) as client:
                yield client
        if request.param == "dependency_provider":
            dp = get_extension(container, GrpcProxy)
            yield dp.get_dependency(Mock(context_data={}))

    def test_successful_call(self, container, client, protos, memory_exporter):

        with entrypoint_waiter(container, "unary_unary"):
            response = client.unary_unary(protos.ExampleRequest(value="A"))
            assert response.message == "A"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.CLIENT, spans))[0]

        assert client_span.status.is_ok
        assert client_span.status.status_code == StatusCode.OK
        assert (
            client_span.attributes["rpc.grpc.status_code"]
            == nameko_grpc.errors.StatusCode.OK.value[0]
        )

    def test_errored_call(self, container, client, protos, memory_exporter):

        with entrypoint_waiter(container, "unary_unary"):
            with pytest.raises(GrpcError):
                client.unary_unary(
                    protos.ExampleRequest(value="A", delay=1), timeout=0.1
                )

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        client_span = list(filter(lambda span: span.kind == SpanKind.CLIENT, spans))[0]

        assert not client_span.status.is_ok
        assert client_span.status.status_code == StatusCode.ERROR
        assert "DEADLINE_EXCEEDED" in client_span.status.description
        assert (
            client_span.attributes["rpc.grpc.status_code"]
            == nameko_grpc.errors.StatusCode.DEADLINE_EXCEEDED.value[0]
        )


class TestServerStatus:
    @pytest.fixture
    def container(self, grpc_port, protos, services, container_factory):

        grpc = Grpc.implementing(services.exampleStub)

        class Error(Exception):
            pass

        class ExampleService:
            name = "example"

            @grpc
            def unary_unary(self, request, context):
                message = request.value * (request.multiplier or 1)
                time.sleep(request.delay / 10)
                return protos.ExampleReply(message=message)

            @grpc
            def unary_error(self, request, context):
                raise Error("boom")

        container = container_factory(ExampleService)
        container.start()

        yield container

        container.stop()

    @pytest.fixture
    def client(self, grpc_port, container, services):
        with Client(
            "//localhost:{}".format(grpc_port), services.exampleStub,
        ) as client:
            yield client

    def test_successful_call(self, container, client, protos, memory_exporter):

        with entrypoint_waiter(container, "unary_unary"):
            response = client.unary_unary(protos.ExampleRequest(value="A"))
            assert response.message == "A"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert server_span.status.is_ok
        assert server_span.status.status_code == StatusCode.OK
        assert (
            server_span.attributes["rpc.grpc.status_code"]
            == nameko_grpc.errors.StatusCode.OK.value[0]
        )

    def test_errored_call(self, container, client, protos, memory_exporter):

        with entrypoint_waiter(container, "unary_error"):
            with pytest.raises(GrpcError):
                client.unary_error(protos.ExampleRequest(value="A"))

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert not server_span.status.is_ok
        assert server_span.status.status_code == StatusCode.ERROR
        assert server_span.status.description == "Error: boom"
        assert (
            server_span.attributes["rpc.grpc.status_code"]
            == nameko_grpc.errors.StatusCode.UNKNOWN.value[0]
        )


class TestScrubbing:
    @pytest.fixture
    def container(self, container_factory, services, protos):

        grpc = Grpc.implementing(services.exampleStub)

        class Service:
            name = "service"

            @grpc
            def sensitive(self, request, context):
                return protos.SensitiveReply(
                    token="should-be-scrubbed", value=request.secret
                )

        container = container_factory(Service)
        container.start()

        return container

    @pytest.fixture
    def client(self, grpc_port, container, services):
        with Client(
            "//localhost:{}".format(grpc_port), services.exampleStub,
        ) as client:
            yield client

    def test_response_scrubbing(self, protos, container, client, memory_exporter):

        with entrypoint_waiter(container, "sensitive"):
            response = client.sensitive(protos.SensitiveRequest(secret="input-secret"))
            assert response.token == "should-be-scrubbed"
            assert response.value == "input-secret"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert (
            server_span.attributes["rpc.grpc.response"]
            == "{'token': 'scrubbed', 'value': 'input-secret'}"
        )

    def test_request_scrubbing(self, protos, container, client, memory_exporter):

        with entrypoint_waiter(container, "sensitive"):
            response = client.sensitive(
                protos.SensitiveRequest(secret="input-secret", not_secret="not-secret")
            )
            assert response.token == "should-be-scrubbed"
            assert response.value == "input-secret"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert (
            server_span.attributes["rpc.grpc.request"]
            == "{'secret': 'scrubbed', 'notSecret': 'not-secret'}"
        )

    def test_metadata_scrubbing(self, protos, container, client, memory_exporter):

        with entrypoint_waiter(container, "sensitive"):
            response = client.sensitive(
                protos.SensitiveRequest(secret="input-secret"),
                metadata=[("password", "scrub-me")],
            )
            assert response.token == "should-be-scrubbed"
            assert response.value == "input-secret"

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2

        server_span = list(filter(lambda span: span.kind == SpanKind.SERVER, spans))[0]

        assert "'password': 'scrubbed'" in server_span.attributes["context_data"]
