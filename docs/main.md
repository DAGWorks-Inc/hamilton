# Welcome to Hamilton's documentation!
<div align="left">
    <a href="https://join.slack.com/t/hamilton-opensource/shared_invite/zt-1bjs72asx-wcUTgH7q7QX1igiQ5bbdcg" target="_blank"><img src="https://img.shields.io/badge/Join-Hamilton_Slack-brightgreen?logo=slack" alt="Hamilton Slack"/></a>
    <a href="https://twitter.com/hamilton_os" target="_blank"><img src="https://img.shields.io/twitter/url/http/shields.io.svg?style=social" alt="Twitter"/></a>
    <a href="https://pepy.tech/project/sf-hamilton" target="_blank"><img src="https://pepy.tech/badge/sf-hamilton" alt="Total Downloads"/></a>
</div>

Hamilton is a general purpose micro-framework for creating dataflows from python functions!
[Please star it here!](https://github.com/dagworks-inc/hamilton).

Specifically, Hamilton defines a novel paradigm, that allows you to specify a flow of (delayed) execution, that forms a Directed Acyclic Graph (DAG).
It was originally built to solve creating wide (1000+) column dataframes. Core to the design of Hamilton is a clear mapping of
function name to dataflow output. That is, Hamilton forces a certain paradigm with writing functions, and aims for DAG clarity,
easy modifications, with always unit testable and naturally documentable code.

# Why should you use Hamilton?
Hamilton's design goal is to make it easier for teams to maintain code that expresses dataflows, a.k.a. pipelines, or workflows.
You should use Hamilton if you want a structured and opinionated way to maintain these types of python code bases.

Here's a quick overview of benefits that Hamilton provides as compared to other tools:

| Feature                                   | Hamilton | Macro orchestration systems (e.g. Airflow) | Feast | DBT | Dask |
|-------------------------------------------|:---:|:---------------------------------------------:|:-----:|:---:|:------:|
| Python 3.7+                               | ✅  |                   ✅                          |   ✅  | ✅  |  ✅    |
| Helps you structure your code base        | ✅  |                   ❌                          |   ❌  | ✅  |  ❌    |
| Code is always unit testable              | ✅  |                   ❌                          |   ❌  | ❌  |  ❌    |
| Documentation friendly                    | ✅  |                   ❌                          |   ❌  | ❌  |  ❌    |
| Can visualize lineage easily              | ✅  |                   ❌                          |   ❌  | ✅  |  ✅    |
| Is just a library                         | ✅  |                   ❌                          |   ❌  | ❌  |  ✅    |
| Runs anywhere python runs                 | ✅  |                   ❌                          |   ❌  | ❌  |  ✅    |
| Built for managing python transformations | ✅  |                   ❌                          |   ❌  | ❌  |  ❌    |
| Replaces macro orchestration systems      | ❌  |                   ✅                          |   ❌  | ❌  |  ❌    |
| Is a feature store                        | ❌  |                   ❌                          |   ✅  | ❌  |  ❌    |


# How can you get started?
If you're in a hurry, we recommend trying Hamilton directly in your browser by going to [tryhamilton.dev](https://www.tryhamilton.dev).
[tryhamilton.dev](https://www.tryhamilton.dev) utilizes pyodide and runs python in the browser - note, a good
internet connection is recommended.

Otherwise hop on over to our [getting started guide](getting-started/index.rst).
