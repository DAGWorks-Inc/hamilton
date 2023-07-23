======================
Cache Node Computation
======================

Sometimes it is convenient to cache intermediate nodes. This is especially useful during development.

For example, if a particular node takes a long time to calculate (perhaps it extracts data from an outside source or performs some heavy computation), you can annotate it with "cache" tag. The first time the DAG is executed, that node will be cached to disk. If then you do some development on any of the downstream nodes, the subsequent executions will load the cached node instead of repeating the computation.

See the full tutorial `here <https://github.com/DAGWorks-Inc/hamilton/tree/main/examples/caching_nodes>`_.
