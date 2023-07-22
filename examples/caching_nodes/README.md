# Caching Graph Adapter

You can use `CachingGraphAdapter` to cache certain nodes.

This is great for:

1. Iterating during development, where you don't want to recompute certain expensive function calls.
2. Providing some lightweight means to control recomputation in production, by controlling whether a "cached file" exists or not.

For iterating during development, the general process would be:

1. Write your functions.
2. Mark them with `tag(cache="SERIALIZATION_FORMAT")`
3. Use the CachingGraphAdapter and pass that to the Driver to turn on caching for these functions.
    a. If at any point in your development you need to re-run a cached node, you can pass
       its name to the adapter in the `force_compute` argument. Then, this node and its downstream
       nodes will be computed instead of loaded from cache.
4. When no longer required, you can just skip (3) and any caching behavior will be skipped.
