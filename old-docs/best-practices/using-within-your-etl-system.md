---
description: How to integrate Hamilton within your existing ETL system.
---

# Using within your ETL System

## Compatibility Matrix

|                                                                           |                                                                                                                                                                                                                                                                                                 |
| ------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Framework / Scheduler                                                     | Compatibility                                                                                                                                                                                                                                                                                   |
| [Airflow](http://airflow.org/)                                            | ✅                                                                                                                                                                                                                                                                                               |
| [Dagster](https://dagster.io/)                                            | ✅                                                                                                                                                                                                                                                                                               |
| [Prefect](https://prefect.io/)                                            | ✅                                                                                                                                                                                                                                                                                               |
| [Kubeflow Pipelines](https://www.kubeflow.org/docs/components/pipelines/) | ✅                                                                                                                                                                                                                                                                                               |
| [CRON](https://en.wikipedia.org/wiki/Cron)                                | ✅                                                                                                                                                                                                                                                                                               |
| [dbt](https://getdbt.com/)                                                | ✅                                                                                                                                                                                                                                                                                               |
| [kubernetes](https://kubernetes.io/)                                      | ✅  but you need to setup kubernetes to run an image that can run python code - e.g. see [https://medium.com/avmconsulting-blog/running-a-python-application-on-kubernetes-aws-56609e7cd88c](https://medium.com/avmconsulting-blog/running-a-python-application-on-kubernetes-aws-56609e7cd88c)  |
| [docker](https://www.docker.com/)                                         | ✅  but you need to setup a docker image that can execute python code.                                                                                                                                                                                                                           |
| ... in general if it runs python 3.6+ ...                                 | ✅                                                                                                                                                                                                                                                                                               |

## &#x20;ETL Recipe

1. Write Hamilton functions & “_driver_” code.
2.  Publish your Hamilton functions in a package,

    or import via other means (e.g. checkout a repository & include in python path).
3. Include _sf-hamilton_ as a python dependency
4. Have your ETL system execute your “driver” code.
5. Profit.
