========================
Using Data Adapters
========================

This is an index of all the available data adapters, both savers and loaders.
Note that some savers and loaders are the same (certain classes can handle both),
but some are different. You will want to reference this when calling out to any of the following:

1. Using :doc:`/reference/decorators/save_to/`.
2. Using :doc:`/reference/decorators/load_from/`.
3. Using :doc:`materializers </reference/drivers/Driver/>`

To read these tables, you want to first look at the key to determine which format you want --
these should be human-readable and familiar to you. Then you'll want to look at the `types` field
to figure out which is the best for your case (the object you want to load from or save to).

Finally, look up the adapter params to see what parameters you can pass to the data adapters.
The optional params come with their default value specified.

If you want more information, click on the `module`, it will send you to the code that implements
it to see how the parameters are used.

As an example, say we wanted to save a pandas dataframe to a CSV file. We would first find the
key `csv`, which would inform us that we want to call `save_to.csv` (or `to.csv` in the case
of `materialize`). Then, we would look at the `types` field, finding that there is a pandas
dataframe adapter. Finally, we would look at the `params` field, finding that we can pass
`path`, and (optionally) `sep` (which we'd realize defaults to `,` when looking at the code).

All together, we'd end up with:

.. code-block:: python

    import pandas as pd
    from hamilton.function_modifiers import value, save_to

    @save_to.csv(path=value("my_file.csv"))
    def my_data(...) -> pd.DataFrame:
        ...

And we're good to go!

If you want to extend these, see :doc:`/reference/io/available-data-adapters` for documentation,
and `the example <https://github.com/DAGWorks-Inc/hamilton/blob/main/examples/materialization/README.md>`_
in the repository for an example of how to do so.

=============
Data Loaders
=============

.. data_adapter_table:: loader

=============
Data Savers
=============

.. data_adapter_table:: saver
