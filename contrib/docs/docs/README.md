---
title: "Introduction"
sidebar_position: 1
---

# Hamilton Dataflows

<h3> Welcome!</h3>

Here you'll find a website that curates a collection of Hamilton Dataflows that are
ready to be used in your own projects. They are user-contributed and maintained, with
the goal of making it easier for you to get started with Hamilton.

We expect this collection to grow over time, so check back often! As dataflows become mature we
will move them into the official sub-package of this site and become maintained by the
Hamilton team.

## Navigation
👈 On the left hand you'll have the ability to find user and official dataflows.
COMING SOON: search & filtering by tags.

## Usage
There are two methods to get access to dataflows presented here.

Assumptions:

1. You are familiar with Hamilton and have it installed. If not, take
[15 minutes to learn Hamilton in your browser](https://www.tryhamilton.dev/) and then `pip install sf-hamilton` to get started.
Come back here when you're ready to use Hamilton.
2. You have the requisite python dependencies installed on your system.
You'll get import errors if you don't. Don't know what you need? Scroll to the bottom of a dataflow to find the requirements. We're working on convenience functions to help!

For more extensive documentation, please see [Hamilton User Contrib documentation](https://hamilton.dagworks.io).

### Static installation
This approach relies on you installing the package on your system. This is the recommended path for
production purposes as you can version-lock your dependencies.

To install the package, run:

```bash
pip install sf-hamilton-contrib==0.0.1rc1
```

Once installed, you can import the dataflows as follows.

Things you need to know:
1. Whether it's a user or official dataflow. If user, what the name of the user is.
2. The name of the dataflow.
```python
from hamilton import driver
# from hamilton.contrib.official import NAME_OF_DATAFLOW
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

### Dynamic installation
Here we dynamically download the dataflow from the internet and execute it. This is useful for quickly
iterating in a notebook and pulling in just the dataflow you need.

```python
from hamilton import dataflow, driver

# download into ~/.hamilton/dataflows and load the module -- WARNING: ensure you know what code you're importing!
# NAME_OF_DATAFLOW = dataflow.import_module("NAME_OF_DATAFLOW") # if official dataflow
NAME_OF_DATAFLOW = dataflow.import_module("NAME_OF_DATAFLOW", "NAME_OF_USER")
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

## How to contribute

If you have a dataflow that you would like to share with the community, please submit a pull request
to this repository. We will review your dataflow and if it meets our standards we will add it to the
package. To submit a pull request please use [this link](https://github.com/dagworks-inc/pulls) as it'll take you to the specific PR template.

### Dataflow standards
We want to ensure that the dataflows in this package are of high quality and are easy to use. To that end,
we have a set of standards that we expect all dataflows to meet. If you have any questions, please reach out.

Standards:
- The dataflow must be a valid Python module.
- It must not do anything malicious.
- It must be well documented.
- It must work.
- It must follow our standard structure as outlined below.


### Checklist for new dataflows:
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

# Got questions?
Join our [slack](https://join.slack.com/t/hamilton-opensource/shared_invite/zt-1bjs72asx-wcUTgH7q7QX1igiQ5bbdcg) community to chat/ask Qs/etc.
