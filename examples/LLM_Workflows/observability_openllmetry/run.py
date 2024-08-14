import openai


def llm_client() -> openai.OpenAI:
    return openai.OpenAI()


def universal_truth(llm_client: openai.OpenAI) -> str:
    response = llm_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a benevolent all-knowning being"},
            {"role": "user", "content": "Please center my HTML <div> tag"},
        ],
    )
    return str(response.choices[0].message.content)


if __name__ == "__main__":
    import __main__  # noqa: I001
    from hamilton import driver
    from hamilton.plugins import h_opentelemetry

    # We're using Traceloop because it can be conveniently set up in 2 lines of code
    # import the `Traceloop` object and initialize it
    from traceloop.sdk import Traceloop

    Traceloop.init()

    # If you wanted to use another OpenTelemetry destination such as the open-source Jaeger,
    # setup the container locally and use the following code

    # from opentelemetry import trace
    # from opentelemetry.sdk.trace import TracerProvider
    # from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    # from opentelemetry.exporter.jaeger import JaegerExporter

    # jaeger_exporter = JaegerExporter(agent_host_name='localhost', agent_port=5775)
    # span_processor = SimpleSpanProcessor(jaeger_exporter)
    # provider = TracerProvider(active_span_processor=span_processor)
    # trace.set_tracer_provider(provider)

    dr = (
        driver.Builder()
        .with_modules(__main__)
        .with_adapters(h_opentelemetry.OpenTelemetryTracer())
        .build()
    )

    results = dr.execute(["universal_truth"])
    print(results["universal_truth"])
