# Running the Hamilton & the Hamilton UI in Snowflake

This example is code for the ["Observability of Python code and application logic with Hamilton UI on Snowflake Container Services" post](https://medium.com/@pkantyka/observability-of-python-code-and-application-logic-with-hamilton-ui-on-snowflake-container-services-a26693b46635) by
[Greg Kantyka](https://medium.com/@pkantyka).

Here we show the code required to be packaged up for use on Snowflake:

1. Docker file that runs the Hamilton UI and a flask endpoint to exercise Hamilton code
2. my_functions.py - the Hamilton code that is exercised by the flask endpoint
3. pipeline_endpoint.py - the flask endpoint that exercises the Hamilton code

To run see:
 - snowflake.sql that contains all the SQL to create the necessary objects in Snowflake and exercise things.

For more details see ["Observability of Python code and application logic with Hamilton UI on Snowflake Container Services" post](https://medium.com/@pkantyka/observability-of-python-code-and-application-logic-with-hamilton-ui-on-snowflake-container-services-a26693b46635) by
[Greg Kantyka](https://medium.com/@pkantyka).
