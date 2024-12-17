-- For more details visit:
-- https://medium.com/@pkantyka/observability-of-python-code-and-application-logic-with-hamilton-ui-on-snowflake-container-services-a26693b46635

CREATE OR REPLACE IMAGE REPOSITORY images;
-- then docker build -t <repository_url>/snowflake-hamilton-ui .
-- docker login <registry_hostname> -u <snowflake-username>
-- docker push <repository_url>/snowflake-hamilton-ui
SHOW IMAGES IN IMAGE REPOSITORY <name>;

CREATE STAGE hamilton_base ENCRYPTION = (type = 'SNOWFLAKE_SSE');


CREATE SERVICE public.hamilton_ui
IN COMPUTE POOL TEST_POOL
FROM SPECIFICATION $$
spec:
  containers:
  - name: hamiltonui
    image: <account-url-registry-host>/<db-name>/<schema-name>/<repo-name>/snowflake-hamilton-ui
    volumeMounts:
    - name: hamilton-basedir
      mountPath: /hamilton-basedir
  endpoints:
   - name: entrypoint
     port: 8001
   - name: hamilton
     port: 8241
     public: true
  volumes:
   - name: hamilton-basedir
     source: "@<db-name>.<schema-name>.hamilton_base"
  $$
QUERY_WAREHOUSE = <warehouse-name>
;

CALL SYSTEM$GET_SERVICE_STATUS('<db-name>.<schema>.hamilton_ui');

CALL SYSTEM$GET_SERVICE_LOGS('<db-name>.<schema>.hamilton_ui', '0', 'hammiltonui', 1000);

SHOW ENDPOINTS IN SERVICE public.hamilton_ui;

CREATE OR REPLACE FUNCTION public.hamilton_pipeline (prj_id number, signups variant, spend variant, output_columns variant)
  RETURNS VARIANT
  SERVICE=public.hamilton_ui
  ENDPOINT=entrypoint
  AS '/echo';


SELECT
  public.hamilton_pipeline (
    1,
    [1, 10, 50, 100, 200, 400],
    [10, 10, 20, 40, 40, 50],
    [ 'spend', 'signups', 'spend_std_dev', 'spend_zero_mean_unit_variance' ]
  ) as data;

WITH input_data AS (
  SELECT
      public.hamilton_pipeline (
        1,
        [1, 10, 50, 100, 200, 400],
        [10, 10, 20, 40, 40, 50],
        [ 'spend', 'signups', 'spend_std_dev', 'spend_zero_mean_unit_variance' ]
      ) as data
),
flattened AS (
  SELECT
    key AS metric_key,
    value AS metric_value
  FROM
    input_data
    left join LATERAL FLATTEN(input_data.data)
)
SELECT
  *
FROM
  flattened f;

WITH input_data AS (
  SELECT
      public.hamilton_pipeline (
        1,
        [1, 10, 50, 100, 200, 400],
        [10, 10, 20, 40, 40, 50],
        [ 'spend', 'signups', 'spend_std_dev', 'spend_zero_mean_unit_variance' ]
      ) as data
),
flattened AS (
  SELECT
    key AS metric_key,
    value AS metric_value
  FROM
    input_data
    left join LATERAL FLATTEN(input_data.data)
)
SELECT
  f2.key,
  f2.value
FROM
  flattened f
  left join lateral flatten(metric_value) f2
where
  metric_key = 'spend_zero_mean_unit_variance';
