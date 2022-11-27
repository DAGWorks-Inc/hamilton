# Hamilton and DBT

In this example, we're going to show you how easy it is to run Hamilton inside a dbt task. Making use of DBT's exciting new
[python API](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/python-models), we can blend the two frameworks seamlessly.

While they may look to have similar purposes at first glance, DBT + Hamilton are actually quite complementary.

- DBT is best at managing SQL logic + handling materialization, while Hamilton excels at managing transforms in python
- DBT contains its own orchestration capabilities, whereas Hamilton will often rely on an external framework to run the code
- DBT does not model micro-level transformations, whereas Hamilton thrives at enabling a user to specify transformations in a readable, maintainable way.
- DBT is focused on analytic/warehouse-level transformations, whereas Hamilton can thrive at expressing ML-specific transforms.

At a high-level, this example shows how DBT can help you get the data/run large-scale operations in your warehouse,
while Hamilton can help you make a model out of it.

To demonstrate this, we've taken one of our favorite examples of writing data science code [xLaszlo's code quality for DS tutorial](https://github.com/xLaszlo/CQ4DS-notebook-sklearn-refactoring-exercise),
and re-written it using a combination of DBT + Hamilton. This models the classic titanic problem.

While the initial example is very simple, it should be enough for you to get started on your own!
# Running

To run the example, you'll need to do two things:

1. Install the dependencies
```bash
# Using pypi
$ cd examples/dbt
$ pip install - r requirements.txt
```
2. Execute!
```bash
# Currently this has to be run from within the directory
$ dbt run
00:53:20  Running with dbt=1.3.1
00:53:20  Found 2 models, 0 tests, 0 snapshots, 0 analyses, 292 macros, 0 operations, 0 seed files, 0 sources, 0 exposures, 0 metrics
00:53:20
00:53:20  Concurrency: 1 threads (target='dev')
00:53:20
00:53:20  1 of 2 START sql table model main.raw_passengers ............................... [RUN]
00:53:20  1 of 2 OK created sql table model main.raw_passengers .......................... [OK in 0.06s]
00:53:20  2 of 2 START python table model main.predict ................................... [RUN]
00:53:21  2 of 2 OK created python table model main.predict .............................. [OK in 0.73s]
00:53:21
00:53:21  Finished running 2 table models in 0 hours 0 minutes and 0.84 seconds (0.84s).
00:53:21
00:53:21  Completed successfully
00:53:21
00:53:21  Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2
```

This will modify a [duckdb file](data/database.duckdb). You can inspect the results using python or your favorite duckdb interface.

# Details

We've organized the code into two separate DBT models:
1. [raw_passengers](models/raw_passengers.sql) This is a simple select and join using duckdb and DBT. Due to the simplicity of DBT -- its just as you would write if it were embedded within a python program, or you were executing SQL on your own!
   It does, however, automatically get materialized.
2. [predict](models/predict.py)
    This uses the data outputted by (1) to do quite a few things:

   - feature engineering to extract a test/train set
   - train a model using the train set
   - run inference over an inference set

    It outputs the inference set. Note it only runs a subset of the DAG -- we could easily add more tasks that output metrics, etc... We just wanted to keep it simple.
    Also note a few oddities in the python model (well-documented) -- including imports. DBT in python is still in beta, and we'll be opening issues/conributing to get it more advanced!

# Future Directions

This is just a start, and we think that Hamilton + DBT have a long/exciting future together. In particular, we could:

1. Compile Hamilton to DBT for orchestration -- the new [SQL adapter](https://github.com/stitchfix/hamilton/issues/197) we're working on would compile nicely to a dbt task.
2. Add more natural integration -- including a dbt plugin for a hamilton task
3. Add more examples with different SQL dialects/different python dialects. _hint_: _we're looking for contributors..._

If you're excited by any of this, drop on by! Some resources to get you help:
- [Hamilton slack channel](https://join.slack.com/t/hamilton-opensource/shared_invite/zt-1ko5snvxt-7KFPTMJyZTw1T7_Gpxryvw)
- [DBT support](https://docs.getdbt.com/docs/dbt-support)
- [xLaszlo's CQ4DS discord](https://discord.gg/8uUZNMCad2)
