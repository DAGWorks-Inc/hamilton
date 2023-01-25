=========================
Available Result Builders
=========================

Use ``from hamilton import base`` to use these Result Builders:

.. list-table::
   :header-rows: 1

   * - Name
     - What it does
     - When you'd use it
   * - `base.DictResult <https://github.com/stitchfix/hamilton/blob/main/hamilton/base.py#L31>`_
     - It returns the results as a dictionary, where the keys map to outputs requested, and values map to what was computed for those values.
     - | When you want to:
       | 1. debug dataflows.
       | 2. have heterogenous return types.
       | 3. Want to manually create a return type.
   * - `base.PandasDataFrameResult <https://github.com/stitchfix/hamilton/blob/main/hamilton/base.py#L39>`_
     - It returns the results as a Pandas Dataframe, where the columns map to outputs requested, and values map to what was computed for those values. Note: this only works if the computed values are pandas series, or scalar values.
     - Use this when you want to create a pandas dataframe.
   * - `base.NumpyMatrixResult <https://github.com/stitchfix/hamilton/blob/main/hamilton/base.py#L49>`_
     - It returns the results as a numpy matrix, where the columns map to outputs requested (in order), and values map to what was computed for those values. Note: this only works if the computed values are all numpy arrays of the **same length and type**, or scalar values. Scalar values will be made to fill the entire column.
     - Use this when you want to create a numpy matrix of results.
