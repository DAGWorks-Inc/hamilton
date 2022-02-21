---
description: Function Naming is something to focus on
---

# Function Naming

Here are three important points about function naming:

1. It enables you to define your Hamilton dataflow.
2. It drives collaboration & code reuse.
3. It serves as documentation itself.&#x20;

You don't need to get this right the first time -- search and replace is really easy with Hamilton code bases -- but it is something to converge thinking on!

## It enables you to define your Hamilton dataflow

The core of Hamilton is really in how you name your functions.

Naming something like

```
def foo_bar(input1: int, input2: pd.Series) -> pd.Series:
    """docs..."""
    ...
```

`foo_bar` is not helpful - it's unclear what this function produces at all. Remember you want function names to mean something, since that will enable clarity when using Hamilton, what is being requested, and will help document what the function itself is doing.

## It drives collaboration and reuse

When people come to encounter your code, they'll need to understand it, add to it, modify it, etc.&#x20;

You'll want to ensure some standardization to enable:

1. Mapping business concepts to function names. E.g. That will help people to find things in the code that map to things that happen within your business.
2. Ensuring naming uniformity across the code base. People usually follow the precedent of the code around them, so if everything in a particular module for say, date features, has a `D_` prefix, then they will likely follow that naming convention. This is likely something you will iterate on -- and it's best to try to converge on a team naming convention once you have a feel for the Hamilton functions being written by the team.

We suggest that long functions names that are separated by `_` aren't a bad thing. E.g. if you were to come across a function named `life_time_value` versus `ltv` versus `l_t_v`, which one is more obvious as to what it is and what it represents?

## It serves as documentation itself&#x20;

Remember your code usually lives a lot longer that you ever think it will. So our suggestion is to always err to the more obvious way of naming to ensure it's clear what a function represents.&#x20;

Again, if you were to come across a function named `life_time_value` versus `ltv` versus `l_t_v`, which one is more obvious as to what it is and what it represents?

