# nameko-grpc-opentelemetry

OpenTelemetry instrumentation for nameko-grpc. To be paired with https://github.com/nameko/opentelemetry-instrumentation-nameko.


## Usage

Inside your codebase, before the service is started, apply the instrumentation:

``` python
# Instrument nameko and nameko-grpc
from nameko_opentelemetry import NamekoInstrumentor
from nameko_grpc_opentelemetry import NamekoInstrumentor

config = dict(
    # configure nameko-grpc entrypoint adapter, and any others
    entrypoint_adapters={
        "nameko_grpc.entrypoint.Grpc": "nameko_grpc_instrumentation.GrpcEntrypointAdapter",
        "my.custom.EntrypointType": "my.custom.EntrypointAdapter"
    },
    # turn on logging of request headers and context data
    send_headers=True,
    # turn on logging of request payloads and entrypoint arguments
    send_request_payloads=True,
    # turn on logging of response payloads and entrypoint results
    send_response_payloads=True,
    # change the default length at which headers and payloads are truncated
    max_truncate_length=1000,
)

NamekoInstrumentor().instrument(**config)
NamekoGrpcInstrumentor().instrument(**config)

# You will also want to instrument any other libraries you're using, e.g.
from opentelemetry.instrumentation.requests import RequestsInstrumentor
RequestsInstrumentor().instrument()

# All entrypoints, and all built-in dependency providers and clients are now instrumented.
# To use the instrumentation somehow, attach a span processor, e.g.
from opentelemetry import trace
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
trace.get_tracer_provider().add_span_processor(
    SimpleSpanProcessor(ConsoleSpanExporter())
)
```