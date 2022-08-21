import analysis_flow

from hamilton import base, driver

"""
Run this analysis by doing `python run_analysis.py`.

Or copy this code into a notebook for further analysis...

Otherwise this is meant to mirror analysis as presented in
https://github.com/numpy/numpy-tutorials/blob/main/content/tutorial-air-quality-analysis.md

"""

if __name__ == "__main__":
    # let's create a dictionary result -- since we want to get a few things from execution for inspection
    adapter = base.SimplePythonGraphAdapter(base.DictResult())
    # adapter = base.SimplePythonGraphAdapter(base.NumpyMatrixResult())  # could also get a numpy matrix back.
    dr = driver.Driver(
        {
            "input_file_name": "air-quality-data.csv",
            "after_lock_date": "2020-03-24T00",
            "before_lock_date": "2020-03-21T00",
        },
        analysis_flow,
        adapter=adapter,
    )

    output = ["t_value", "p_value", "before_sample", "after_sample"]
    # dr.visualize_execution(output, './my_file.dot', {})
    result = dr.execute(output)
    print(result)
    print(f"The t value is {result['t_value']} and the p value is {result['p_value']}.")

    # Just to show you, from a dict result, it's easy to use another ResultMixin to build another result.
    # This can be an easy way to try out/prototype what you want to do next -- before committing to it.
    sample_matrix = base.NumpyMatrixResult().build_result(
        before_sample=result["before_sample"], after_sample=result["after_sample"]
    )
    print(sample_matrix)
