---
description: How to build your own Result Builder
---

# Custom Result Builders

### Before starting

We suggest reaching out on [discord](https://discord.gg/wCqxqBqn73) or via a github issue, if you have a use case for a custom result builder. Knowing about your use case and talking through it can help ensure we aren't duplicating effort, and we can help steer you in the right direction.

### What you need to do

You need to implement a class that implements a single function - see [github code](https://github.com/stitchfix/hamilton/blob/main/hamilton/base.py#L18-L28):

```
class ResultMixin(object):
    """Base class housing the static function.

    Why a static function? That's because certain frameworks can only pickle a static function, not an entire
    object.
    """
    @staticmethod
    @abc.abstractmethod
    def build_result(**outputs: typing.Dict[str, typing.Any]) -> typing.Any:
        """This function builds the result given the computed values."""
        pass
```

E.g.

```
import typing
from hamilton import base
class MyCustomBuilder(base.ResultMixin):
     # add a constructor if you need to
     @staticmethod
     def build_result(**outputs: typing.Dict[str, typing.Any]) -> YOUR_RETURN_TYPE:
         """Custom function you fill in"""
         # your logic would go here
         return OBJECT_OF_YOUR_CHOOSING
```

### How to use it

You would then pair that with a graph adapter that takes in a ResultMixin object. E.g. `SimplePythonGraphAdapter`. See [available-graph-adapters.md](../api-reference/available-graph-adapters.md "mention") for which ones take in a custom ResultMixin object.
