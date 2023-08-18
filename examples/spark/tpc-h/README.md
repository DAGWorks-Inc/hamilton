# TPC-H

We've represented a few TPC-h queries using pyspark + hamilton.

While we have not optimized these for benchmarking, they provide a good set of examples for how to express pyspark logic/break
it into hamilton functions.

## Running

To run, you have `run.py` -- this enables you to run a few of the queries. That said, you'll have to generate the data on your own, which is a bit tricky.

Download dbgen here, and follow the instructions: https://www.tpc.org/tpch/. You can also reach out to us and we'll help you get set up.
