# subdag operator

This README demonstrates the use of the subdag operator.

The subdag operator allows you to effectively run a driver within a node.
In this case, we are calculating unique website visitors from the following set of parameters:

1. Region = CA (canada) or US (United States)
2. Granularity of data = (day, week, month)

You can find the code in [unique_users.py](unique_users.py) and [reusable_subdags.py](reusable_subdags.py)
and look at how we run it in [main.py](main.py).
