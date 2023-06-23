# Welcome to Hamilton's documentation!
<div align="left">
    <a href="https://join.slack.com/t/hamilton-opensource/shared_invite/zt-1bjs72asx-wcUTgH7q7QX1igiQ5bbdcg" target="_blank"><img src="https://img.shields.io/badge/Join-Hamilton_Slack-brightgreen?logo=slack" alt="Hamilton Slack"/></a>
    <a href="https://twitter.com/hamilton_os" target="_blank"><img src="https://img.shields.io/twitter/url/http/shields.io.svg?style=social" alt="Twitter"/></a>
    <a href="https://pepy.tech/project/sf-hamilton" target="_blank"><img src="https://pepy.tech/badge/sf-hamilton" alt="Total Downloads"/></a>
</div>

Hamilton is a general purpose micro-framework for creating dataflows from python functions!
[Please star it here!](https://github.com/dagworks-inc/hamilton).

Hamilton is a novel paradigm for specifying a flow of delayed execution in python. It was originally built to simplify the creation of wide (1000+) column dataframes, but works on python objects of any type and dataflows of any complexity. Core to the design of Hamilton is a clear mapping of function name to components of the generated artifact, allowing you to quickly grok the relationship between the code you write and the data you produce. This paradigm makes modifications easy to build and track, ensures code is self-documenting, and makes it natural to unit test your data transformations. When connected together, these functions form a [Directed Acyclic Graph](https://en.wikipedia.org/wiki/Directed_acyclic_graph) (DAG), which the Hamilton framework can execute, optimize, and report on.

# Why should you use Hamilton?
Hamilton's design goal is to make it easier for teams to maintain code that expresses dataflows, a.k.a. pipelines, or workflows.
You should use Hamilton if you want a structured and opinionated way to maintain these types of python code bases.

Here's a quick overview of benefits that Hamilton provides as compared to other tools:

| Feature                                    | Hamilton | Macro orchestration systems (e.g. Airflow) | Feast | DBT | Dask |
|--------------------------------------------|:---:|:---------------------------------------------:|:-----:|:---:|:----:|
| Python 3.7+                                | ✅  |                   ✅                          |   ✅  | ✅  |  ✅   |
| Helps you structure your code base         | ✅  |                   ❌                          |   ❌  | ✅  |  ❌   |
| Code is always unit testable               | ✅  |                   ❌                          |   ❌  | ❌  |  ❌   |
| Documentation friendly                     | ✅  |                   ❌                          |   ❌  | ❌  |  ❌   |
| Can visualize lineage easily               | ✅  |                   ❌                          |   ❌  | ✅  |  ✅   |
| Is just a library                          | ✅  |                   ❌                          |   ❌  | ❌  |  ✅   |
| Runs anywhere python runs                  | ✅  |                   ❌                          |   ❌  | ❌  |  ✅   |
| Built for managing python transformations  | ✅  |                   ❌                          |   ❌  | ❌  |  ❌   |
| Can model GenerativeAI/LLM based workflows | ✅  |                   ❌                          |   ❌  | ❌  |  ❌   |
| Replaces macro orchestration systems       | ❌  |                   ✅                          |   ❌  | ❌  |  ❌   |
| Is a feature store                         | ❌  |                   ❌                          |   ✅  | ❌  |  ❌   |


# How can you get started?
Hop on over to our [getting started guide](getting-started/index.rst)!
