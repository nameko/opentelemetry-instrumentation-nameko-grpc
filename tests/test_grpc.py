from unittest.mock import Mock

import grpc
import pytest
from nameko.testing.services import entrypoint_waiter
from nameko.testing.utils import get_extension
from nameko_grpc.client import Client
from nameko_grpc.dependency_provider import GrpcProxy
from nameko_grpc.entrypoint import Grpc
from nameko_grpc.errors import GrpcError
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode

from nameko_opentelemetry import active_tracer


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

    def test_client_method_not_found(self, protos, container, client, memory_exporter):

        with pytest.raises(GrpcError) as error:
            client.not_found(protos.ExampleRequest(value="hello"))
        assert error.value.code == grpc.StatusCode.UNIMPLEMENTED
        assert error.value.message == "Method not found!"

        container.stop()

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
    pass


class TestResultAttributes:
    pass


class TestNoTracer:
    pass


class TestExceptions:
    pass


class TestClientStatus:
    pass


class TestServerStatus:
    pass


class TestScrubbing:
    pass
