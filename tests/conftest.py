# -*- coding: utf-8 -*-
import nameko
import pytest
from grpc import protos_and_services
from nameko.testing.utils import find_free_port
from nameko_grpc.client import Client
from nameko_grpc.entrypoint import Grpc
from nameko_grpc_opentelemetry import NamekoGrpcInstrumentor
from opentelemetry import trace
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from wrapt import wrap_function_wrapper

from nameko_opentelemetry import NamekoInstrumentor


@pytest.fixture(scope="session")
def memory_exporter():
    return InMemorySpanExporter()


@pytest.fixture(scope="session")
def trace_provider(memory_exporter):
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(memory_exporter))
    trace.set_tracer_provider(provider)


@pytest.fixture
def config():
    return {
        "send_headers": True,
        "send_request_payloads": True,
        "send_response_payloads": True,
        "entrypoint_adapters": {
            "nameko_grpc.entrypoint.Grpc": "nameko_grpc_opentelemetry.GrpcEntrypointAdapter"
        },
    }


@pytest.fixture(autouse=True)
def instrument(trace_provider, memory_exporter, config):
    nameko_instrumentor = NamekoInstrumentor()
    grpc_instrumentor = NamekoGrpcInstrumentor()

    nameko_instrumentor.instrument(**config)
    grpc_instrumentor.instrument(**config)
    yield
    memory_exporter.clear()
    nameko_instrumentor.uninstrument()
    grpc_instrumentor.uninstrument()


@pytest.fixture
def protos():
    protos, _ = protos_and_services("tests/example.proto")
    return protos


@pytest.fixture
def services():
    _, services = protos_and_services("tests/example.proto")
    return services


@pytest.fixture(autouse=True)
def grpc_port():
    port = find_free_port()
    with nameko.config.patch({"GRPC_BIND_PORT": port}):
        yield port
