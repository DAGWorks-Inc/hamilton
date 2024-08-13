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
    from traceloop.sdk import Traceloop

    import __main__
    from hamilton import driver
    from hamilton.plugins import h_opentelemetry

    Traceloop.init()

    dr = (
        driver.Builder()
        .with_modules(__main__)
        .with_adapters(h_opentelemetry.OpenTelemetryTracer(tracer_name=__name__))
        .build()
    )

    results = dr.execute(["universal_truth"])
    print(results["universal_truth"])
