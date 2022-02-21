---
description: How to integrate Hamilton within your existing ETL system.
---

# Using within your ETL System

## Compatibility Matrix

|                                           |                              |
| ----------------------------------------- | ---------------------------- |
| Framework / Scheduler                     | Compatibility                |
| Airflow                                   | ✅                            |
| Dagster                                   | ✅                            |
| Prefect                                   | ✅                            |
| Kubeflow                                  | ✅                            |
| CRON                                      | ✅                            |
| dbt                                       | ⛔️ (dbt does not run python) |
| ... in general if it runs python 3.6+ ... | ✅                            |

## &#x20;ETL Recipe

1. Write Hamilton functions & “_driver_” code.
2.  Publish your Hamilton functions in a package,

    or import via other means (e.g. checkout a repository & include in python path).
3. Include _sf-hamilton_ as a python dependency
4. Have your ETL system execute your “driver” code.
5. Profit.
