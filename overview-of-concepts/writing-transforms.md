# Writing Transforms

Hamilton's novel approach to writing transforms differentiates it from other frameworks with the same goal. In particular, the following properties of hamilton enable creation of readable, flexible pipelines:

#### Utilizing python's native function API to specify transform shape

In order to represent data pipelines, Hamilton makes use of every piece of a python function. Let's dig into the components of a python function, and how Hamilton uses each one of them to define a transform:

| Function Component        | usage                                        |   |
| ------------------------- | -------------------------------------------- | - |
| function name             | used as the node's name for access/reference |   |
| parameter names           | transform's upstream dependencies            |   |
| parameter type annotation | data types of upstream dependencies          |   |
| return annotation         | type of data produced by transform           |   |
| docstring                 | documentation for the node                   |   |
| function body             | implementation of the node                   |   |

#### Storing the structure of the pipeline along with its implementation

The structure of the pipeline hamilton defines is largely coupled with the implementation of its nodes. At first glance, this approach runs counter to standard software engineering principles. In fact, one of hamilton's creators initially disliked this so much that he created a rival prototype called _Burr_ (that looks a lot like [prefect](https://www.prefect.io) or [dagster](https://docs.dagster.io/getting-started) with fully reusible, delayed components. Both of them presented this to the customer Data Science team, who ultimately appreciated the elegant simplicity of Hamilton's approach. So, why couple the implementation of a transform to where it lives in the pipeline?

1. **It greatly improves readability of pipelines**. The ratio of reading to writing code can be as high as [10:1,](https://www.goodreads.com/quotes/835238-indeed-the-ratio-of-time-spent-reading-versus-writing-is) especially for complex data pipelines, so optimizing for readability is very high-value.&#x20;
2. **It reduces the cost to create and maintains pipelines**. Rather than making changes or getting started in two places (the definition and the invocation of a transform), we can just change the node!

OK, but this still doesn't address the problem of reuse. How can we make our code [DRY](https://en.wikipedia.org/wiki/Don't\_repeat\_yourself), while maintaining all these great properties? _**In Hamilton, this is not an inherent trade-off.**_ By using [decorators](decorators.md), [helper functions](writing-transforms.md#storing-the-structure-of-the-pipeline-along-with-its-implementation), and [configuration parameters](parametrizing-the-dag.md), you can both have your cake and eat it too.

### Modules and Helper Functions

Pipelines use every function in a module, so you don't have to list the elements of your pipeline individually. In order to promote code reuse, however, hamilton allows you to specify helper functions by ignoring functions that start with an underscore. Consider the following (very simple) pipeline:

```python
import pandas as pd

def _add_series(series_1: pd.Series, series_2: pd.Series) -> pd.Series:
    return series_1 + series_2

def foo_plus_bar(bar: pd.Series, foo: pd.Series) -> pd.Series:
    return _add_series(foo, bar)
```

The only node is `foo_plus_bar` (not counting the required inputs `foo`or `bar`). `_add_series` is a helper function that is not loaded into hamilton.\
