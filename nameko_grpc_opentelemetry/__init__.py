# -*- coding: utf-8 -*-
import sys
import warnings
from functools import partial
from weakref import WeakKeyDictionary

import nameko_grpc.client
import nameko_grpc.entrypoint
import nameko_grpc.errors
from google.protobuf.json_format import MessageToDict
from nameko_grpc.constants import Cardinality
from nameko_grpc.errors import GrpcError
from nameko_grpc.inspection import Inspector
from nameko_opentelemetry import active_tracer
from nameko_opentelemetry.entrypoints import EntrypointAdapter
from nameko_opentelemetry.scrubbers import scrub
from nameko_opentelemetry.utils import serialise_to_string
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.propagate import inject
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.util._time import _time_ns
from wrapt import wrap_function_wrapper

from nameko_grpc_opentelemetry.package import _instruments
from nameko_grpc_opentelemetry.tee import Teeable


active_spans = WeakKeyDictionary()
result_iterators = WeakKeyDictionary()


class GrpcEntrypointAdapter(EntrypointAdapter):
    def get_attributes(self, worker_ctx):
        attributes = super().get_attributes(worker_ctx)

        inspector = Inspector(worker_ctx.entrypoint.stub)

        attributes.update(
            {
                "rpc.system": "grpc",
                "rpc.method": worker_ctx.entrypoint.method_name,
                "rpc.service": inspector.service_name,
                "rpc.grpc.cardinality": worker_ctx.entrypoint.cardinality.name,
            }
        )
        return attributes

    def get_call_args_attributes(self, worker_ctx, call_args, redacted):
        # get request directly from worker context rather than `call_args`, in case
        # the name is different
        request, context = worker_ctx.args

        if self.config.get("send_request_payloads"):

            cardinality = worker_ctx.entrypoint.cardinality
            if cardinality in (Cardinality.STREAM_UNARY, Cardinality.STREAM_STREAM):
                messages = [
                    serialise_to_string(scrub(MessageToDict(req), self.config))
                    # request can be tee'd here because it's not been read yet
                    for req in request.tee()
                ]
                request_string = " | ".join(messages)

            else:

                request_string = serialise_to_string(
                    scrub(MessageToDict(request), self.config)
                )

            return {"rpc.grpc.request": request_string}

    def get_result_attributes(self, worker_ctx, result):
        attributes = {}

        if self.config.get("send_response_payloads"):
            cardinality = worker_ctx.entrypoint.cardinality
            if cardinality in (Cardinality.UNARY_STREAM, Cardinality.STREAM_STREAM):
                messages = []
                # result is tee'd in handle_result because the service
                # has already drained the iterator by the time we get here
                try:
                    messages.extend(
                        serialise_to_string(scrub(MessageToDict(res), self.config))
                        for res in result_iterators.pop(worker_ctx, {})
                        if res is not None
                    )
                except Exception as exc:
                    messages.append(f"{type(exc).__name__}: {exc}")

                response_string = " | ".join(messages)

            else:
                (res,) = result
                if res is not None:
                    response_string = serialise_to_string(
                        scrub(MessageToDict(res), self.config)
                    )
                else:
                    response_string = ""

            attributes.update({"rpc.grpc.response": response_string})

        return attributes

    def get_status(self, worker_ctx, result, exc_info):
        """ Span status for this worker.
        """
        if exc_info:
            return super().get_status(worker_ctx, result, exc_info)

        request, context = worker_ctx.args
        status = int(context.response_stream.trailers.get("grpc-status", 0))
        if status:
            return Status(
                StatusCode.ERROR,
                description=context.response_stream.trailers.get("grpc-message"),
            )

        return Status(StatusCode.OK)

    def get_exception_attributes(self, worker_ctx, exc_info):
        """ Additional attributes to save alongside a worker exception.
        """
        attributes = super().get_exception_attributes(worker_ctx, exc_info)
        attributes.update(
            {"exception.message": GrpcError.from_exception(exc_info).message}
        )
        return attributes

    def end_span(self, span, worker_ctx, result, exc_info):
        if not span.is_recording():
            return

        cardinality = worker_ctx.entrypoint.cardinality

        # capture exceptions, either direct or from a stream
        capture_exc_info = exc_info
        if cardinality in (Cardinality.UNARY_STREAM, Cardinality.STREAM_STREAM):
            try:
                list(result_iterators[worker_ctx].tee())
            except Exception:
                capture_exc_info = sys.exc_info()
                # super() impl won't call get_result_attributes in the
                # error case, but we want it if the error occurred in a stream.
                span.set_attributes(
                    self.get_result_attributes(worker_ctx, result) or {}
                )

        # set grpc status code attribute
        if capture_exc_info:
            grpc_error = GrpcError.from_exception(capture_exc_info)
            span.set_attribute("rpc.grpc.status_code", grpc_error.code.value[0])
        else:
            _, context = worker_ctx.args
            span.set_attribute(
                "rpc.grpc.status_code",
                int(context.response_stream.trailers.get("grpc-status", 0)),
            )

        super().end_span(span, worker_ctx, result, capture_exc_info)


def future(tracer, config, wrapped, instance, args, kwargs):
    """ Wrap nameko_grpc.client.Method.future

    Start a span...
    """
    method = instance
    inspector = Inspector(method.client.stub)

    cardinality = inspector.cardinality_for_method(method.name)

    attributes = {
        "rpc.system": "grpc",
        "rpc.grpc.status_code": nameko_grpc.errors.StatusCode.OK.value[0],
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
    state = {}

    try:
        return wrapped(*args, **kwargs)
    except GrpcError:
        state["exc_info"] = sys.exc_info()
        raise
    finally:  # pragma: no cover -- branch coverage gets confused
        activated = active_spans.get(instance)
        if activated:
            activation, span = activated

            if "exc_info" in state:
                exc_info = state["exc_info"]
                span.set_attribute("rpc.grpc.status_code", exc_info[1].code.value[0])
                activation.__exit__(*exc_info)
            else:
                span.set_status(Status(StatusCode.OK))
                activation.__exit__(None, None, None)
            span.end(_time_ns())
        else:
            # something went wrong when starting the span; nothing more to do
            warnings.warn("result when no active span")


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
        # create a tee immediately, before the entrypoint starts draining the iterator
        result_iterators[worker_ctx] = result.tee()

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

        wrap_function_wrapper(
            "nameko_grpc.entrypoint",
            "Grpc.handle_result",
            partial(handle_result, tracer, config),
        )

        if config.get("send_request_payloads"):
            wrap_function_wrapper(
                "nameko_grpc.entrypoint",
                "Grpc.handle_request",
                partial(entrypoint_handle_request, tracer, config),
            )

    def _uninstrument(self, **kwargs):
        unwrap(nameko_grpc.client.Method, "future")
        unwrap(nameko_grpc.client.Future, "result")
        unwrap(nameko_grpc.entrypoint.GrpcServer, "handle_request")
        unwrap(nameko_grpc.entrypoint.Grpc, "handle_request")
        unwrap(nameko_grpc.entrypoint.Grpc, "handle_result")
