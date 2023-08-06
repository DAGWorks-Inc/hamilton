===========================
Hamilton Functions
===========================

Hamilton's novel approach to writing transform functions differentiates it from other frameworks with the same goal. In
particular, the following properties of Hamilton enable creation of readable, flexible dataflows:

All transforms are simple python functions
--------------------------------------------------

In order to represent dataflows, Hamilton leans heavily on python's native function definition API. Let's dig into the
components of a python function, and how Hamilton uses each one of them to define a transform:

.. list-table::
   :header-rows: 1

   * - Function Component
     - usage
   * - function name
     - used as the node's name for access/reference
   * - parameter names
     - function's upstream dependencies
   * - parameter type annotation
     - data types of upstream dependencies
   * - return annotation
     - type of data produced by the function
   * - docstring
     - documentation for the node
   * - function body
     - implementation of the node

.. _storing-the-structure-of-the-dataflow:

Dataflow structure == implementation
-------------------------------------------------------------------

The structure of the dataflow Hamilton defines is largely coupled with the implementation of its nodes. At first glance,
this approach runs counter to standard software engineering principles of decoupling components. In fact, one of Hamilton's creators initially[
disliked this so much that he created a rival prototype called *Burr* (that looks a lot like
`prefect <https://www.prefect.io/>`_ or `dagster <https://docs.dagster.io/getting-started>`_ with fully reusable,
delayed components). Both of them presented this to the customer Data Science team, who ultimately appreciated the
elegant simplicity of Hamilton's approach. So, why couple the implementation of a transform to where it lives in the
dataflow?

#. **It greatly improves readability of dataflows**. The ratio of reading to writing code can be as high as `10:1 <https://www.goodreads.com/quotes/835238-indeed-the-ratio-of-time-spent-reading-versus-writing-is>`_, especially for complex dataflows, so optimizing for readability is very high-value.
#. **It reduces the cost to create and maintains dataflows**. Rather than making changes or getting started in two places (the definition and the invocation of a transform), we can just change the node, i.e. function!

OK, but this still doesn't address the problem of reuse. How can we make our code
`DRY <https://en.wikipedia.org/wiki/Don't\_repeat\_yourself>`_, while maintaining all these great properties? **With
the Hamilton framework, this is not an inherent trade-off.** By using :doc:`decorators <decorators-overview>`
and :ref:`parameterizing your dataflow <parameterizing-the-dag>`, you can both have your cake and eat it too.

Dynamic DAGs
----------------------------

Hamilton supports limited dynamism in DAG execution. While we believe that most workflows should be somewhat static
(and dynamism should be orchestrated external to the definition), we support the ability to expand execution over a set of
previous nodes. You do this by marking a function that yields a generator of type `Foo` as having an output type `Parallelizable[Foo]`, and
downstream marking the input type as `Collect[input]`. Note that, currently, multiple functions can declare, as an input, the output of a `Parallelizable`
function, whereas only one node can currently feed into `Collect`.

For example:

.. code-block:: python

    from hamilton.htypes import Parallelizable, Collect

    def site_to_crawl() -> Parallelizable[str]:
        for item in ["hamilton.readthedocs.io", "tryhamilton.dev", "..."]:
            yield item


    def loaded_homepage(site_to_crawl: str) -> str:
        return _load(site_to_crawl)

    def home_page_size(loaded_homepage: str) -> int:
        return _get_size(load_homepage)

    def total_size(home_page_size: Collect[int]) -> int:
        return sum(home_page_size)

In this case the `site_to_crawl` function indicates that we should run `loaded_homepage` and `home_page_size`
with each value that `site_to_crawl` returns. The `Collect` type indicates that we join all of the previous items.

Note that, to use this, you'll need to construct the hamilton driver with the `Builder`, and call `(allow_experimental_mode=True)`.

See :doc:`customizing execution </concepts/customizing-execution>` for more details.

Modules and Helper Functions
----------------------------

Hamilton constructs dataflows using every function in a module, so you don't have to list the elements of your pipeline
individually. In order to promote code reuse, however, hamilton allows you to specify helper functions by ignoring
functions that start with an underscore. Consider the following (very simple) pipeline:

.. code-block:: python

    import pandas as pd

    def _add_series(series_1: pd.Series, series_2: pd.Series) -> pd.Series:
        return series_1 + series_2

    def foo_plus_bar(bar: pd.Series, foo: pd.Series) -> pd.Series:
        return _add_series(foo, bar)

The only node is ``foo_plus_bar`` (not counting the required inputs ``foo`` or ``bar``). ``_add_series`` is a helper
function that is not loaded into Hamilton.
