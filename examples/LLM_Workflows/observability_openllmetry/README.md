# Monitor Hamilton with OpenTelemetry, OpenLLMetry and Traceloop

In this simple example, you'll learn how to use `OpenTelemetryTracer` to trace your Hamilton code, in particular LLM applications.

![Traceloop screenshot](image.png)

[OpenTelemetry](https://opentelemetry.io/) is an open-source cross-language tool that allows to instrument, generate, collect, and export telemetry data (metrics, logs, traces). When building applications and data platforms with several components, OpenTelemetry helps you centralize information about your system allowing you to monitor performance.

[OpenLLMetry](https://github.com/traceloop/openllmetry) is an open-source Python library that automatically instruments with OpenTelemetry components of your LLM stack including LLM providers (OpenAI, Anthropic, HuggingFace, Cohere, etc.), vector databases (Weaviate, Qdrant, Chroma, etc.), and frameworks ([Burr](https://github.com/dagworks-inc/burr), Haystack, LangChain, LlamaIndex). In concrete terms, it means you automatically get detailed traces of API calls, retrieval operations, or text transformations for example.

One thing to note, OpenTelemetry is a middleware; it doesn't provide a destination to store data nor a dashboard. For the purpose of this example, we'll use the tool [Traceloop](https://www.traceloop.com/) which has a generous free-tier and is built by the developers of OpenLLMetry.

## Set up
Having access to a [Traceloop account](https://www.traceloop.com/) and an API key is a pre-requisite. The example can also be adapted to suit other [OpenTelemetry-compatible destinations](https://opentelemetry.io/ecosystem/vendors/).

1. Create a virtual environment and activate it
    ```bash
    python -m venv venv && . venv/bin/active
    ```

2. Install requirements.
    ```bash
    pip install -r requirements.txt
    ```

3. Set environment variables for your API keys `OPENAI_API_KEY` and `TRACELOOP_API_KEY`

4. Execute the code
    ```bash
    python run.py
    ```

5. Explore results on Traceloop (or your OpenTelemetry destination).


## How does it compare to the Hamilton UI

On one hand, OpenTelemetry collects lightweight metadata about dataflow and node executions. It provides a central location for traces when your system has multiple componentes (e.g., server, client, cache, database). On the other hand, the Hamilton UI is tailored to Hamilton and provides visualizations, data lineage, summary statistics, and more utilities to improve your development experience. Given these systems emit lightweight metadata, it's totally reasonable to use both!
