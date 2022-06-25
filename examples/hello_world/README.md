# Classic Hamilton Hello World

In this example we show you how to create a simple hello world dataflow that
creates a dataframe as a result. It performs a series of transforms on the
input to create columns that appear in the output.

File organization:

* `my_functions.py` houses the logic that we want to compute. Note how the functions are named, and what input
parameters they require. That is how we create a DAG modeling the dataflow we want to happen.
* `my_script.py` houses how to get Hamilton to create the DAG and exercise it with some inputs.

To run things:
```bash
> python my_script.py
```

If you have questions, or need help with this example,
join us on [slack](https://join.slack.com/t/hamilton-opensource/shared_invite/zt-1bjs72asx-wcUTgH7q7QX1igiQ5bbdcg), and we'll try to help!
