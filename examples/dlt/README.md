# dlt

This example shows dlt + Hamilton can help you cover the full ELT cycle using portable Python code. It is a companion to this [documentation page](https://hamilton.dagworks.io/en/latest/integrations/dlt/).

# Content
It includes a pipeline to ingest messages from Slack channels and generate threads summaries.

- `slack/` is the code imported from dlt for the Slack `Source`
- `.dlt/` contains the source's version, config, and secrets for the dlt `Pipeline`
- `transform.py` contains the Hamilton code to transform data and build the `threads` table
- `run.py` contains the code to execution the dlt `Pipeline` and the Hamilton dataflow.

# Set up
1. Create a virtual environment and activate it
    ```console
    python -m venv venv && . venv/bin/active
    ```

2. Install requirements
    ```console
    pip install -r requirements.txt
    ```
3. Follow this dlt [guide to ingest Slack data](https://dlthub.com/docs/dlt-ecosystem/verified-sources/slack). Make sure to invite your Slack app to your channel!

4. Execute the code
    ```console
    python run.py
    ```

# References
- dlt to ingest [Slack data](https://dlthub.com/docs/dlt-ecosystem/verified-sources/slack)
- Reconstructing Slack threads [docs](https://api.slack.com/messaging/retrieving#finding_threads)
