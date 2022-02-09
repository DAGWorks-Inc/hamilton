---
description: >-
  The open source framework Hamilton (https://github.com/stitchfix/hamilton),
  originally built to manage and run Stitch Fix's data pipelines.
---

# Hamilton

## Getting Started

If you want to jump in head first, we have a simple tutorial for getting started!&#x20;

{% content-ref url="15-minutes-to-mastery/" %}
[15-minutes-to-mastery](15-minutes-to-mastery/)
{% endcontent-ref %}

## What is Hamilton?

Hamilton is a framework that allows for delayed executions of functions in a Directed Acyclic Graph (DAG). It was created to solve the problem of creating complex data pipelines. Core to the design of Hamilton is a clear mapping of function name to implementation. With this, Hamilton forces a certain paradigm with writing functions, and aims for DAG clarity, easy modifications, unit testing, and documentation.

Hamilton's method of defining data pipelines presents a new paradigm when it comes to creating datasets. Rather than manipulating one (or a set of) central dataframes procedurally and extracting the data you want, Hamilton enables you to run your pipeline with the following steps:

1. Define the pipeline as a set of transforms using Hamilton's API&#x20;
2. Specify parameter values your pipeline requires
3. Specify which outputs you want to compute from your pipeline

The rest is delegated to the framework, which handles the computation for you.

Let's illustrate this with some code. If you were asked to write a simple transform (let's use pandas for the sake of argument), you may decide to write something simple like this:

```python
df['col_c'] = df['col_a'] + df['col_b']
```

To represent this in a way hamilton can understand, you write:

```python
def col_c(col_a: pd.Series, col_b: pd.Series) -> pd.Series:
    """Creating column c from summing column a and column b."""
    return col_a + col_b
```

![The above code represented as a diagram](.gitbook/assets/image.png)

The hamilton framework takes the above code, forms it into a computational DAG, and executes it for you!

## User Guide

Dive a little deeper and start exploring our API reference to get an idea of everything that's possible with the API:

{% content-ref url="reference/api-reference/" %}
[api-reference](reference/api-reference/)
{% endcontent-ref %}
