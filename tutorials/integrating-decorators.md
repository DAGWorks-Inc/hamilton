---
description: Let's talk about some functionality
---

# Integrating Decorators

Hamilton relies on[ python decorators](https://towardsdatascience.com/the-simplest-tutorial-for-python-decorator-dadbf8f20b0f) to enable easy code reuse. Taking the previous example, let's say that we cared about the running average spend per signup with both a 2 and a 3 week lookback. Rather than writing a bunch of functions with almost exactly the same definitions, we can parametrize! The following uses two decorator to [curry](https://en.wikipedia.org/wiki/Currying) your nodes into multiple functions.

{% code title="with_decorators.py" %}
```python
import pandas as pd

from hamilton import function_modifiers


@function_modifiers.parametrized(
    'rolling_lookback',
    assigned_output={
        ('avg_2wk_spend', "Average marketing spend looking back 2 weeks"): 2,
        ('avg_3wk_spend', "Average marketing spend looking back 3 weeks"): 3,
    }
)
def avg_nwk_spend(spend: pd.Series, rolling_lookback: int) -> pd.Series:
    """Rolling week average spend, parametrized by rolling_lookback"""
    return spend.rolling(rolling_lookback).mean()


@function_modifiers.parametrized_input(
    'spend',
    {
        'avg_2wk_spend': ("acquisition_cost_2wk", "Spend per signup with a two week rolling average "),
        'avg_3wk_spend': ("acquisition_cost_3wk", "Spend per signup with a three week rolling average")
    }
)
def acquisition_cost(spend: pd.Series, signups: pd.Series) -> pd.Series:
    """The cost per signup in relation to spend."""
    return spend / signups
```
{% endcode %}

All we have to do is modify our driver to run the right module and ask for the right outputs, and we're good to go!

{% code title="driver.diff" %}
```git
@@ -6 +6 @@ import pandas as pd
-import my_functions
+import with_decorators
@@ -21 +21 @@ if __name__ == '__main__':
-    dr = driver.Driver(initial_columns, my_functions)  # can pass in multiple modules
+    dr = driver.Driver(initial_columns, with_decorators)  # can pass in multiple modules
@@ -26,2 +26,2 @@ if __name__ == '__main__':
-        'avg_3wk_spend',
-        'acquisition_cost',
+        'acquisition_cost_2wk',
+        'acquisition_cost_3wk'
```
{% endcode %}

Running the driver now gives you the following:\


```
   spend  signups  acquisition_cost_2wk  acquisition_cost_3wk
0     10        1                   NaN                   NaN
1     10       10                1.0000                   NaN
2     20       50                0.3000              0.266667
3     40      100                0.3000              0.233333
4     40      200                0.2000              0.166667
5     50      400                0.1125              0.108333
```

bash
