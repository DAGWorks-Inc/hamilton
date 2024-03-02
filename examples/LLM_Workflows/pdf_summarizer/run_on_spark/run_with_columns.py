"""Spark driver and Hamilton driver code."""

import spark_pdf_pipeline
from pyspark.sql import SparkSession

from hamilton import base, driver, log_setup


def my_spark_job(spark: SparkSession, openai_gpt_model: str, content_type: str, user_query: str):
    """Template for a Spark job that uses Hamilton for their featuring engineering, i.e. any map, operations.

    :param spark: the SparkSession
    :param openai_gpt_model: the model to use for summarization
    :param content_type: the content type of the document to summarize
    :param user_query: the user query to use for summarization
    """
    dr = (
        driver.Builder()
        .with_config({"file_type": "pdf"})
        .with_modules(spark_pdf_pipeline)
        .with_adapter(base.DefaultAdapter())
        .build()
    )
    # create inputs to the UDFs - this needs to be column_name -> spark dataframe.
    execute_inputs = {
        "spark_session": spark,
        "save_path": "summarized_pdf_df.parquet",
        "openai_gpt_model": openai_gpt_model,
        "content_type": content_type,
        "user_query": user_query,
    }
    output = ["saved_summarized_pdf_df"]
    # visualize execution of what is going to be appended
    dr.visualize_execution(
        output,
        "./spark_with_columns_summarization.png",
        inputs=execute_inputs,
        deduplicate_inputs=True,
    )
    # tell Hamilton to tell Spark what to do
    dict_result = dr.execute(output, inputs=execute_inputs)
    return dict_result["saved_summarized_pdf_df"]


if __name__ == "__main__":
    import os

    openai_api_key = os.environ.get("OPENAI_API_KEY")
    log_setup.setup_logging(log_level=log_setup.LOG_LEVELS["INFO"])
    # create the SparkSession -- note in real life, you'd adjust the number of executors to control parallelism.
    spark = SparkSession.builder.config(
        "spark.executorEnv.OPENAI_API_KEY", openai_api_key
    ).getOrCreate()
    spark.sparkContext.setLogLevel("info")
    # run the job
    _df = my_spark_job(spark, "gpt-3.5-turbo-0613", "Scientific article", "Can you ELI5 the paper?")
    # show the dataframe & thus make spark compute something
    _df.show()
    spark.stop()
