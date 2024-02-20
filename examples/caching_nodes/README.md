Here you'll find two adapters that allow you to cache the results of your functions.

The first one is the `DiskCacheAdapter`, which uses the `diskcache` library to store the results on disk.

The second one is the `CachingGraphAdapter`, which requires you to tag functions to cache along with the
serialization format.

Both have their sweet spots and trade-offs. We invite you play with them and provide feedback on which one you prefer.
