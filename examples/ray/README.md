# Scaling Hamilton on Ray

[Ray](https://ray.io) is a general purpose framework that allows for parallel
computation on a local machine, as well as scaling to a
Ray cluster.

# Two ways to use Ray

## injecting @ray.remote
See the `hello_world` example for how you might accomplish
scaling Hamilton on Ray, where `@ray.remote` is injected around
each Hamilton function.

## creating tasks
For [this paralle task approach](https://hamilton.dagworks.io/en/latest/concepts/parallel-task/) see [this
example](https://github.com/DAGWorks-Inc/hamilton/tree/main/examples/LLM_Workflows/scraping_and_chunking) instead.
