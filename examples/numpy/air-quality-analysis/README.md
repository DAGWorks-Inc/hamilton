# Air Quality Analysis

This is taken from the numpy tutorial https://github.com/numpy/numpy-tutorials/tree/main/content.

# analysis_flow.py
Is where the analysis steps are defined as Hamilton functions.

Versus doing this analysis in a notebook, the strength of Hamilton here is in
forcing concise definitions and language around steps in the analysis -- and
then magically the analysis is pretty reusable / very easy to augment. E.g. add some
@config.when or split things into python modules to be swapped out, to extend the
analysis to new data sets, or new types of analyses.

# run_analysis.py
Is where the driver code lives to create the DAG and exercise it.

To exercise it:
> python run_analysis.py
