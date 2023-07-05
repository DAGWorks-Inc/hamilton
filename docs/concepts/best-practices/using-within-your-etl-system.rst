============================
Using within your ETL System
============================

Conceptually you can integrate Hamilton within your existing ETL system quite easily:

Compatibility Matrix
--------------------

.. list-table:: Title
   :header-rows: 1

   * - Framework / Scheduler
     - Compatibility
   * - `Airflow <http://airflow.org>`_
     - ✅
   * - `Dagster <https://dagster.io>`_
     - ✅
   * - `Prefect <https://prefect.io>`_
     - ✅
   * - `Kubeflow Pipelines <https://www.kubeflow.org/docs/components/pipelines>`_
     - ✅
   * - `CRON <https://en.wikipedia.org/wiki/Cron>`_
     - ✅
   * - `dbt <https://getdbt.com>`_
     - ❔  (dbt did not run python, but now it does so it should)
   * - `kubernetes <https://kubernetes.io>`_
     - ✅ but you need to setup kubernetes to run an image that can run python code - e.g. see `Running a python application on kubernetes <https://medium.com/avmconsulting-blog/running-a-python-application-on-kubernetes-aws-56609e7cd88c>`_
   * - `docker <https://www.docker.com>`_
     - ✅ but you need to setup a docker image that can execute python code.
   * - ... in general if it runs python 3.6+ ...
     - ✅

ETL Recipe
----------

#. Write Hamilton functions & `“driver”` code.
#. Publish your Hamilton functions in a package, or import via other means (e.g. checkout a repository & include in python path).
#. Include `sf-hamilton` as a python dependency
#. Have your ETL system execute your “driver” code.
#. Profit.
