# Hamilton UI SDK: Client Code &amp; Related

Welcome to using the Hamilton UI!

Here are instructions on how to get started with tracking, and managing your Hamilton
DAGs with the Hamilton UI.

## Getting Started

For the latest documentation, please consult our
[Hamilton documentation](https://hamilton.dagworks.io/) under `Hamilton UI`.

For a quick overview of Hamilton, we suggest [tryhamilton.dev](https://www.tryhamilton.dev/).

## Using the HamiltonTracker

First, you'll need to install the Hamilton SDK package. Assuming you're using pip, you
can do this with:

```bash
# install the package & cli into your favorite python environment.
pip install "sf-hamilton[sdk]"

# And validate -- this should not error.
python -c "from hamilton_sdk import adapters"
```

Next, you'll need to modify your Hamilton driver. You'll only need to use one line of code to
replace your driver with ours:

```python
from hamilton_sdk import adapters
from hamilton import driver

tracker = adapters.HamiltonTracker(
   project_id=PROJECT_ID,  # modify this as needed
   username=YOUR_EMAIL, # modify this as needed
   dag_name="my_version_of_the_dag",
   tags={"environment": "DEV", "team": "MY_TEAM", "version": "X"}
)
dr = (
  driver.Builder()
    .with_config(your_config)
    .with_modules(*your_modules)
    .with_adapters(tracker)
    .build()
)
# to run call .execute() or .materialize() on the driver
```
*Project ID*: You'll need a project ID. Create a project if you don't have one, and take the ID from that.

*username*: This is the email address you used to set up the Hamilton UI.

*dag_name*: for a project, the DAG name is the top level way to group DAGs.
E.g. ltv_model, us_sales, etc.

*tags*: these are optional are string key value paris. They allow you to filter and curate
various DAG runs.

Then run Hamilton as normal! Each DAG run will be tracked, and you'll have access to it in the
Hamilton UI. After spinning up the Hamilton UI application, visit it to see your projects & DAGs.


# License
The code here is licensed under the BSD-3 Clear Clause license. See the main repository [LICENSE](../../LICENSE) for details.
