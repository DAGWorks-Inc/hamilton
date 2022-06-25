# Examples

Here you'll find some very simple hello world type examples.

If you have questions, or need help with these examples,
join us on [slack](https://join.slack.com/t/hamilton-opensource/shared_invite/zt-1bjs72asx-wcUTgH7q7QX1igiQ5bbdcg), and we'll try to help!

## Show casing scaling Pandas
The `hello_world` folder shows a simple example of how to create a Hamilton DAG and run it.

Say you want to scale it? Well then, take a look at the `dask`, `ray`, and `spark` folders.
They each define a `hello_world` folder. Key thing to note, is that their `business_logic.py` files,
are in fact all identical, and symbolic links to the `my_functions.py` in our classic `hello_world` example.
The reason for this, is to show you, that you can infact scale Pandas, and also have your choice of framework
to run it on!

For information on how to run Hamilton on `dask`, `ray`, `spark`, we invite you to read the READMEs in those
folders.

## A reusable scikit-learn model pipeline
Under `model_examples` you'll find a how you could apply Hamilton to model your ML workflow.
Check it out to get a sense for how Hamilton could make your ML pipelines reusable/general
components...
