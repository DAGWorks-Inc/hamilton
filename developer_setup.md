# Developer/Contributor Setup

## Repo organization

This repository is organized as follows:

1. hamilton/ is code to orchestrate and execute the graph.
2. tests/ is the place where unit tests (or light integration tests) are located.

## How to contribute

1. Checkout the repo. If external to Stitch Fix, fork the repo.
2. Create a virtual environment for it. See python algo curriculum slides for details.
3. Activate the virtual environment and install all dependencies. One for the package, one for making comparisons, one for running unit tests. I.e. `pip install -r requirements*.txt` should install all three for you.
3. Make pycharm depend on that virtual environment & install required dependencies (it should prompt you because it'll read the requirements.txt file).
4. `brew install pre-commit` if you haven't.
5. Run `pre-commit install` from the root of the repository.
6. Create a branch off of the latest master branch. `git checkout -b my_branch`.
7. Do you work & commit it.
8. Push to github and create a PR.
9. When you push to github circle ci will kick off unit tests and migration tests (for Stitch Fix users only).


## How to run unit tests

You need to have installed the `requirements-test.txt` dependencies into the environment you're running for this to work. You can run tests two ways:

1. Through pycharm/command line.
2. Using circle ci locally. The config for this lives in `.circleci/config.yml` which also shows commands to run tests
from the command line.

### Using pycharm to execute & debug unit tests

You can debug and execute unit tests in pycharm easily. To set it up, you just hit `Edit configurations` and then
add New > Python Tests > pytest. You then want to specify the `tests/` folder under `Script path`, and ensure the
python environment executing it is the appropriate one with all the dependencies installed. If you add `-v` to the
additional arguments part, you'll then get verbose diffs if any tests fail.

### Using circle ci locally

You need to install the circleci command line tooling for this to work. See the unit testing algo curriculum slides for details.
Once you have installed it you just need to run `circleci local execute` from the root directory and it'll run the entire suite of tests
that are setup to run each time you push a commit to a branch in github.

# Pushing to pypi
These are the steps to push to pypi. This is taken from the [python packaging tutorial](https://packaging.python.org/tutorials/packaging-projects/#generating-distribution-archives).

1. Have an account & be granted the ability to push to sf-hamilton on testpypi & real pypi.
2. Setup API tokens and add them to your ~/.pypirc.
3. Run `python3 -m pip install --upgrade build`.
4. Run `python3 -m pip install --upgrade twine`
5. Push to test pypi - `python3 -m twine upload --repository testpypi dist/*`.

   Note: you cannot push the same version twice to test or real pypi. So it's useful to append `-rcX` to the version.
   Once you're happy, you can remove that; just remember to not check that in.
6. Validate you can install from testpypi - follow the URL output.
7. If you can, then push to real pypi. `python3 -m twine upload dist/*`
8. Double check you can download and install what you just pushed in a fresh environment. A good thing to test is to
   run the hello world example.

# Announcing your release

Now that you've pushed to pypi, announce your release! We plan to automate this, but for now...

1. Create a github release (select auto-generate release for painless text generation). Create a tag that's called `sf-hamilton-{version_string}`.
See [1.2.0](https://github.com/stitchfix/hamilton/releases/tag/sf-hamilton-1.2.0) for an example.
2. Announce on the #releases channel in discord, following the format presented there.
3. Thanks for contributing!
