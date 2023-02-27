========================
Result Builders
========================

Result builders help you augment what is returned by the driver's `execute()` function.

Here we list what is available:

Generic
________________________

.. autoclass:: hamilton.base.ResultMixin
   :members:

.. autoclass:: hamilton.base.DictResult
   :members:


Pandas
________________________

.. autoclass:: hamilton.base.PandasDataFrameResult
   :members: build_result

.. autoclass:: hamilton.base.StrictIndexTypePandasDataFrameResult
   :members: build_result

Numpy
________________________

.. autoclass:: hamilton.base.NumpyMatrixResult
   :members: build_result

Polars
________________________

.. autoclass:: hamilton.plugins.polars_implementations.PolarsDataFrameResult
   :members: build_result
