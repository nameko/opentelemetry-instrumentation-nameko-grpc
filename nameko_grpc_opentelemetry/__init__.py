# -*- coding: utf-8 -*-
from functools import partial
from weakref import WeakKeyDictionary

import nameko_grpc.client
import nameko_grpc.entrypoint
from nameko_grpc.constants import Cardinality
from nameko_grpc.errors import GrpcError
from nameko_grpc.inspection import Inspector
from nameko_grpc_opentelemetry.package import _instruments
from nameko_grpc_opentelemetry.tee import Teeable
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.propagate import inject
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.util._time import _time_ns
from wrapt import wrap_function_wrapper

from nameko_opentelemetry import active_tracer
from nameko_opentelemetry.entrypoints import EntrypointAdapter


active_spans = WeakKeyDictionary()


class GrpcEntrypointAdapter(EntrypointAdapter):
    def get_attributes(self):
        attributes = super().get_attributes()

        inspector = Inspector(self.worker_ctx.entrypoint.stub)

        attributes.update(
            {
                "rpc.system": "grpc",
                "rpc.method": self.worker_ctx.entrypoint.method_name,
                "rpc.service": inspector.service_name,
                "rpc.grpc.cardinality": self.worker_ctx.entrypoint.cardinality.name,
            }
        )
        return attributes

    def get_call_args_attributes(self, call_args, redacted):
        cardinality = self.worker_ctx.entrypoint.cardinality
        return {}

    def get_result_attributes(self, result):
        cardinality = self.worker_ctx.entrypoint.cardinality
        return {}


def future(tracer, config, wrapped, instance, args, kwargs):
    """ Wrap nameko_grpc.client.Method.future

    Start a span...
    """
    method = instance
    inspector = Inspector(method.client.stub)

    cardinality = inspector.cardinality_for_method(method.name)

    attributes = {
        "rpc.system": "grpc",
        # "rpc.grpc.status_code": grpc.StatusCode.OK.value[0],
        "rpc.method": method.name,
        "rpc.service": inspector.service_name,
        "rpc.grpc.cardinality": cardinality.name,
    }

    span = tracer.start_span(
        name=f"{inspector.service_name}.{method.name}",
        kind=trace.SpanKind.CLIENT,
        attributes=attributes,
        start_time=_time_ns(),
    )
    activation = trace.use_span(span)
    activation.__enter__()

    headers = {}
    inject(headers)
    method.extra_metadata.extend((key, value) for key, value in headers.items())

    future = wrapped(*args, **kwargs)

    active_spans[future] = (activation, span)

    return future


def result(tracer, config, wrapped, instance, args, kwargs):
    """ Wrap nameko_grpc.client.Future.result

    Terminate span...
    """
    try:
        return wrapped(*args, **kwargs)
    finally:
        activation, span = active_spans[instance]
        activation.__exit__(None, None, None)
        span.end(_time_ns())


def entrypoint_handle_request(tracer, config, wrapped, instance, args, kwargs):
    """ Wrap nameko_grpc.entrypoint.Grpc.handle_request

    If this entrypoint accepts a streaming request, we need to wrap it in a Teeeable
    instance so that `get_call_args_attributes` doesn't drain the iterator.

    Unfortunately `handle_request` doesn't have access to the iterator directly,
    so we have to wrap the request stream's `consume` method instead.
    """
    request_stream, response_stream = args

    if instance.cardinality in (Cardinality.STREAM_UNARY, Cardinality.STREAM_STREAM):

        original_consume = request_stream.consume

        def consume(input_type):
            return Teeable(original_consume(input_type))

        request_stream.consume = consume

    return wrapped(request_stream, response_stream, **kwargs)


def handle_result(tracer, config, wrapped, instance, args, kwargs):
    """ Wrap nameko_grpc.entrypoint.Grpc.handle_result

    If this entrypoint returns a streaming result, we need to wrap it in a Teeeable
    instance so that `get_result_attributes` doesn't drain the iterator.
    """
    response_stream, worker_ctx, result, exc_info = args

    if instance.cardinality in (Cardinality.UNARY_STREAM, Cardinality.STREAM_STREAM):
        result = Teeable(result)

    return wrapped(response_stream, worker_ctx, result, exc_info, **kwargs)


def server_handle_request(tracer, config, wrapped, instance, args, kwargs):
    """ Wrap nameko_grpc.entrypoint.GrpcServer.handle_request.

    Handle cases where no entrypoint is fired.
    """
    try:
        return wrapped(*args, **kwargs)
    except GrpcError as exc:
        request_stream, response_stream = args

        span = tracer.start_span(
            request_stream.headers.get(":path"), kind=trace.SpanKind.SERVER
        )
        span.set_status(Status(StatusCode.ERROR, description=exc.message))
        with trace.use_span(
            span,
            record_exception=False,
            set_status_on_exception=False,
            end_on_exit=True,
        ):
            raise


class NamekoGrpcInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self):
        return _instruments

    def _instrument(self, **config):
        """
        ...
        """
        tracer = active_tracer()

        wrap_function_wrapper(
            "nameko_grpc.client", "Method.future", partial(future, tracer, config)
        )
        wrap_function_wrapper(
            "nameko_grpc.client", "Future.result", partial(result, tracer, config)
        )
        wrap_function_wrapper(
            "nameko_grpc.entrypoint",
            "GrpcServer.handle_request",
            partial(server_handle_request, tracer, config),
        )

        if config.get("send_request_payloads"):
            wrap_function_wrapper(
                "nameko_grpc.entrypoint",
                "Grpc.handle_request",
                partial(entrypoint_handle_request, tracer, config),
            )

        if config.get("send_response_payloads"):

            wrap_function_wrapper(
                "nameko_grpc.entrypoint",
                "Grpc.handle_result",
                partial(handle_result, tracer, config),
            )

    def _uninstrument(self, **kwargs):
        unwrap(nameko_grpc.client.Method, "future")
        unwrap(nameko_grpc.client.Future, "result")
        unwrap(nameko_grpc.entrypoint.GrpcServer, "handle_request")
        unwrap(nameko_grpc.entrypoint.Grpc, "handle_request")
        unwrap(nameko_grpc.entrypoint.Grpc, "handle_result")