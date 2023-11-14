## Off-the-shelf Hamilton Dataflows

Here you'll find documentation about the sf-hamilton-contrib package that curates a collection of Hamilton Dataflows that are
ready to be used in your own projects. They are user-contributed and maintained, with
the goal of making it easier for you to get started with Hamilton.

We expect this collection to grow over time, so check back often! As dataflows become mature we
will move them into the official sub-package of the respository and become maintained by the
Hamilton team.

### Usage
There are two ways to get access to dataflows in this package. For either approach,
the assumption is that you have the requisite python dependencies installed on your system.
You'll get import errors if you don't. Don't know what you need, we have convenience functions to help!

#### Static installation
This approach relies on you installing the package on your system. This is the recommended path for
production purposes as you can version-lock your dependencies.

To install the package, run:

```bash
pip install sf-hamilton-contrib --upgrade
```

Once installed, you can import the dataflows as follows.

Things you need to know:
1. Whether it's a user or official DAGWorks supported dataflow. If user, what the name of the user is.
2. The name of the dataflow.
```python
from hamilton import driver
# from hamilton.contrib.dagworks import NAME_OF_DATAFLOW
from hamilton.contrib.user.NAME_OF_USER import NAME_OF_DATAFLOW

dr = (
    driver.Builder()
    .with_config({})  # replace with configuration as appropriate
    .with_modules(NAME_OF_DATAFLOW)
    .build()
)
# execute the dataflow, specifying what you want back. Will return a dictionary.
result = dr.execute(
    [NAME_OF_DATAFLOW.FUNCTION_NAME, ...],  # this specifies what you want back
    inputs={...}  # pass in inputs as appropriate
)
```
To find an example [go to the hub](https://hub.dagworks.io/docs/).

#### Dynamic installation
Here we dynamically download the dataflow from the internet and execute it. This is useful for quickly
iterating in a notebook and pulling in just the dataflow you need.

```python
from hamilton import dataflows, driver

# downloads into ~/.hamilton/dataflows and loads the module -- WARNING: ensure you know what code you're importing!
# NAME_OF_DATAFLOW = dataflows.import_module("NAME_OF_DATAFLOW") # if using official DAGWorks dataflow
NAME_OF_DATAFLOW = dataflows.import_module("NAME_OF_DATAFLOW", "NAME_OF_USER")
dr = (
  driver.Builder()
  .with_config({})  # replace with configuration as appropriate
  .with_modules(NAME_OF_DATAFLOW)
  .build()
)
# execute the dataflow, specifying what you want back. Will return a dictionary.
result = dr.execute(
  [NAME_OF_DATAFLOW.FUNCTION_NAME, ...],  # this specifies what you want back
  inputs={...}  # pass in inputs as appropriate
)
```
To find an example [go to the hub](https://hub.dagworks.io/docs/).

#### Modification
Getting started is one thing, but then modifying to your needs is another. So we have a prescribed
flow to enable you to take a dataflow, and copy the code to a place of your choosing. This allows
you to easily modify the dataflow as you see fit.

Run this in a notebook or python script to copy the dataflow to a directory of your choosing.
```python
from hamilton import dataflows

# dynamically pull and then copy
NAME_OF_DATAFLOW = dataflows.import_module("NAME_OF_DATAFLOW", "NAME_OF_USER")
dataflows.copy(NAME_OF_DATAFLOW, destination_path="PATH_TO_DIRECTORY")
# copy from the installed library
from hamilton.contrib.user.NAME_OF_USER import NAME_OF_DATAFLOW
dataflows.copy(NAME_OF_DATAFLOW, destination_path="PATH_TO_DIRECTORY")
```
You can then modify/import the code as you see fit. See [copy()](https://hamilton.dagworks.io/en/latest/reference/dataflows/copy/)
for more details.


### How to contribute

If you have a dataflow that you would like to share with the community, please submit a pull request
to this repository. We will review your dataflow and if it meets our standards we will add it to the
package. To submit a pull request please use [this template](https://github.com/DAGWorks-Inc/hamilton/blob/main/.github/PULL_REQUEST_TEMPLATE/HAMILTON_CONTRIB_PR_TEMPLATE.md)
. To access it, create a new Pull Request, then hit the `preview` tab, and click the link to append `template=HAMILTON_CONTRIB_PR_TEMPLATE.md` to the URL.

#### Dataflow standards
We want to ensure that the dataflows in this package are of high quality and are easy to use. To that end,
we have a set of standards that we expect all dataflows to meet. If you have any questions, please reach out.

Standards:
- The dataflow must be a valid Python module.
- It must not do anything malicious.
- It must be well documented.
- It must work.
- It must follow our standard structure as outlined below.


#### Checklist for new dataflows:
Do you have the following?
- [ ] Added a directory mapping to my github user name in the contrib/hamilton/contrib/user directory.
  - [ ] If my author names contains hyphens I have replaced them with underscores.
  - [ ] If my author name starts with a number, I have prefixed it with an underscore.
  - [ ] If your author name is a python reserved keyword. Reach out to the maintainers for help.
  - [ ] Added an author.md file under my username directory and is filled out.
  - [ ] Added an __init__.py file under my username directory.
- [ ] Added a new folder for my dataflow under my username directory.
  - [ ] Added a README.md file under my dataflow directory that follows the standard headings and is filled out.
  - [ ] Added a __init__.py file under my dataflow directory that contains the Hamilton code.
  - [ ] Added a requirements.txt under my dataflow directory that contains the required packages outside of Hamilton.
  - [ ] Added tags.json under my dataflow directory to curate my dataflow.
  - [ ] Added valid_configs.jsonl under my dataflow directory to specify the valid configurations.
  - [ ] Added a dag.png that shows one possible configuration of my dataflow.

## Got questions?
Join our [slack](https://join.slack.com/t/hamilton-opensource/shared_invite/zt-1bjs72asx-wcUTgH7q7QX1igiQ5bbdcg) community to chat/ask Qs/etc.
