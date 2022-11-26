
/*
  Basic DBT model to load data from the duckdb table
*/

{{ config(materialized='table') }}

SELECT
    tbl_passengers.pid,
    tbl_passengers.pclass,
    tbl_passengers.sex,
    tbl_passengers.age,
    tbl_passengers.parch,
    tbl_passengers.sibsp,
    tbl_passengers.fare,
    tbl_passengers.embarked,
    tbl_passengers.name,
    tbl_passengers.ticket,
    tbl_passengers.boat,
    tbl_passengers.body,
    tbl_passengers."home.dest",
    tbl_passengers.cabin,
    tbl_targets.is_survived as survived
FROM
    tbl_passengers
JOIN
    tbl_targets
ON
    tbl_passengers.pid=tbl_targets.pid
