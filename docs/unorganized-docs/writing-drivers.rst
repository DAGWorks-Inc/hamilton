===============
Writing Drivers
===============

Recall that there are two types of drivers you'll interface with: `Framework-specified` and `user-specified`.
Framework-specified drivers will be general-purpose -- these interface with the internals of Hamilton, and handle
passing data in. You will, however, want to write your own user-specified drivers.

At the end of the day, Hamilton is not particularly opinionated around `how` or `when` you run your dataflow. Only
around how you write it. So, writing a driver is up to you. That said, we recommend following some general-purpose
guidelines:

#. Include logic with side-effects in your drivers. E.G. writing to dbs, etc... For ease/reuse of pipelines, we recommend executing side-effect code once, after the pipeline completes. If you need more complex materialization, etc... see read about :doc:`../reference/api-extensions/custom-graph-adapters` -- these allow you to customize execution of nodes.
#. When migrating, build the API to match your current one! E.G. write to the same tables, have the same function signature, etc... This allows you to migrate easily, only changing one component (the dataflow) at a time
#. Control the loading of modules/config within your driver. E.G. which modules are selected, etc... This should function as a shield from the user of your driver (likely you) from the complexity of the underlying DAG.
