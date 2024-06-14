# machine\_learning

This template shows a ML pipeline.

It shows a few things:

1. It shows how one could split up functions into modules. E.g. loading, vs features, vs fitting.
2. It also shows how to use `@subdag` to fit different models in the same DAG run and reuse the same fitting code.
3. It shows how to use data loaders and data savers to load and save data, that also then emit extra metadata
that can be used to track lineage in the UI.
4. It shows how to use the HamiltonTracker to integrate with the Hamilton UI.

## Getting started

To get started, you need to have the Hamilton UI running.

There are two ways to do this:

1. Pip install the Hamilton UI and run it. This is the recommended way to get started.
2. Run the Hamilton UI in a docker container.

### Local Install
You just need to install the following targets:

```bash
pip install "sf-hamilton[ui,sdk]"
```
And then run:
```bash
hamilton ui
# python -m hamilton.cli.__main__ ui # on windows
```

### Docker Install

1. See https://hamilton.dagworks.io/en/latest/concepts/ui/ for details, here are the cliff notes:

    ```bash
    git clone https://github.com/dagworks-inc/hamilton
    cd hamilton/ui/deployment
    ./run.sh
    ```
   Then go to http://localhost:8242 and create (1) an email, and (2) a project.
   See [this video](https://youtu.be/DPfxlTwaNsM) for a walkthrough.

### Run the example

2. Ensure you have the right python dependencies installed.
```bash
cd hamilton/examples/hamilton_ui
pip install -r requirements.txt
```

2. Run the `run.py` script. Providing the email/username, and project ID to be able to log to the Hamilton UI.
```bash
python run.py --email <email> --project_id <project_id>
```
Once you've run that, run this:
```bash
python run.py --email <email> --project_id <project_id> --load-from-parquet True
```
Then you can go see the difference in the Hamilton UI. Find your project under http://localhost:8242/dashboard/projects.

## Things to try:

1. Place an error in the code and see how it shows up in the Hamilton UI. e.g. `raise ValueError("I'm an error")`.
2. In `models.py` change `"data_set": source("data_set_v1"),` to `"data_set": source("data_set_v2"),`, along with
what is requested in `run.py` (i.e. change/add saving `data_set_v2`) and see how the lineage changes in the Hamilton UI.
3. Add a new feature and propagate it through the pipeline. E.g. add a new feature to `features.py` and then to a dataset.
Execute it and then compare the data observed in the Hamilton UI against a prior run.
