# Using Hamilton for ML dataflows

Here we have a simple example showing how you can
write a ML training and evaluation workflow with Hamilton.

`pip install scikit-learn` or `pip install -r requirements.txt` to get the right python dependencies
to run this example.

## Reusable general logic
* `my_train_evaluate_logic.py` houses logic that should be invariant to how hamilton is executed. This contains logic
written in a reusable way.

You can think of the 'inputs' to the functions here, as an interface a "data loading" module would have to fulfill.

## data loaders
* `iris_loader.py` houses logic to load iris data.
* `digit_loader.py` houses logic to load digit data.

The idea is that you'd swap these modules out/create new ones that would dictate what data is loaded.
Thereby enabling you to create the right inputs into your dataflow at DAG construction time. They should
house the same function names as they should map to the inputs required by functions defined in `my_train_evaluate_logic.py`.

## running it
* run.py houses the "driver code" required to stitch everything together. It is responsible for creating the
right configuration to create the DAG, as well as determining what python modules should be loaded.
