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

## Running examples through docker image
Examples could also be executed through the provided docker image.
Each example directory inside docker image contains a `hamilton-env` Python virtual environment.
`hamilton-env` environment contains all the dependencies required to run the example.

NOTE: If you already have the container image you can skip to container creation (step 3)

1. Change directory to `examples`
```bash
cd hamilton/examples
```

2. Build the container image.
```bash
docker build --tag hamilton-example .
```
Docker build takes around `6m16.298s` depending on the system configuration and network.
Alternatively, you can pull the container image from here: ...

3. Creating a container
```bash
docker run -it --rm --name hamilton-example hamilton-example
```

4. Running the `hello_world` example inside the container
```bash
cd hamilton/examples/hello_world
source hamilton-env/bin/activate
python my_script.py
```
