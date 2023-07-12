# Hamilton + Prefect

In this example, were going to show how to run Hamilton within a Prefect task.
- [**Prefect**](https://prefect.io) is an open source orchestrator written in Python. Its purpose is to launch and execute your workflows over your infrastructure. It has integrations with many popular tools (Snowflake, dbt, Ray, OpenAI, etc.)
- [**Hamilton**](https://github.com/dagworks-inc/hamilton) is a micro-framework to describe dataflows in Python. Its strength is expressing the flow of data & computation in a way that is straightforward to create, maintain, and resue (much like dbt for SQL)

## Why both?
Prefect includes useful features for production system such as job queues, caching, retries, logging, and more. However, each additional task you create comes with execution overhead and needs to be manually added to the DAG structure. Opting for less granular tasks ultimately reduces visibility and maintainability.

In contrast, Hamilton automatically builds its DAG from the function definition, encouraging clean, small and single-purposed functions. By using Hamilton in a Prefect task, you gain this greater data lineage, reduce execution overhead, and get portable Python code you can run outside Prefect too.

## File organization
- `run.py` is a script launching a Prefect flow that uses Hamilton in its tasks.
- `prepare_data.py`, `train_model.py`, and `evaluate_model.py` are Python modules containing the function definitions that Hamilton calls.

## Prefect setup
The easiest way to get this example running is to sign up for Prefect's free tier and follow the [Prefect Cloud Quickstart](https://docs.prefect.io/latest/cloud/cloud-quickstart/) section. The steps are:
1. Signup for Prefect
2. Create a workspace
3. In this directory, create a virtual environment with the needed requirements. You can copy the commands below to do so.
    ```
    python -m venv ./venv &&
    . ./venv/bin/activate &&&
    pip install -r requirements.txt
    ```
4. Login to Prefect with your local machine using `prefect cloud login`
5. Execute the workflow by running `python run.py`. You should see a new run appear on your dashboard at https://app.prefect.cloud/

## Tips
1. Use Prefect [Blocks](https://docs.prefect.io/latest/concepts/blocks/) to store your Hamilton configuration. This way, you can edit it directly from your Prefect dashboard to launch different runs without altering your source code.
![blocks](./docs/prefect_config_block.JPG)
2. Prefect keeps track of your flow and tasks config, which is useful to reproduce runs.
![params](./docs/prefect_run_params.JPG)
3. Store Hamilton [DAG visualization](https://hamilton.dagworks.io/en/latest/how-tos/use-hamilton-for-lineage/) with your runs using Prefect [artifacts](https://docs.prefect.io/latest/concepts/artifacts/). 
    ![dag](./docs/prepare_data_hamilton_dag.png)
    The general idea is:
    1. produce a local file
        ```
        dr = hamilton.driver.Driver(...)

        visualization_path = "hamilton_dag"
        # visualize_execution shares a similar signature to Driver.execute()
        dr.visualize_execution(
            final_vars=...,
            inputs=...,
            output_file_path=visualization_path,
            render_kwargs={"format": "png"}, 
        )
        ```
    2. convert file as bytes
        ```
        import b64

        with open(f"{visualization_path}.png", "rb") as f:
            encoded_string = b64.b64encode(f.read())
        ```

    3. string to a remote storage
        ```
        from prefect.filesystems import RemoteFileSystem

        remote_basepath=...
        fs = RemoteFileSystem(basepath=remote_basepath)
        remote_dag_file = ...
        with fs.open(remote_dag_file, "wb") as f:
            f.write(encoded_string)
        ```

    4. link the string to the Prefect run
        ```
        from prefect.artifacts import create_link_artifact

        create_link_artifact(
            key=...,
            link=f"{remote_basepath}/{remote_dag_file},
            description="Hamilton execution DAG"
        )
        ```
