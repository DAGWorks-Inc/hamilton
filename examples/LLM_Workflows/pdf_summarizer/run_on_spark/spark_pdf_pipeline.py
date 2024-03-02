import pandas as pd
import pyspark.sql as ps
import summarization

from hamilton.plugins.h_spark import with_columns


def pdf_df(spark_session: ps.SparkSession) -> ps.DataFrame:
    pandas_df = pd.DataFrame(
        # TODO: update this to point to a PDF or two.
        {"pdf_source": ["CDMS_HAMILTON_PAPER.pdf"]}
    )
    df = spark_session.createDataFrame(pandas_df)
    return df


@with_columns(
    summarization,
    select=["summarized_chunks", "summarized_text"],
    columns_to_pass=["pdf_source"],
    config_required=["file_type"],
)
def summarized_pdf_df(pdf_df: ps.DataFrame) -> ps.DataFrame:
    return pdf_df


def saved_summarized_pdf_df(
    summarized_pdf_df: ps.DataFrame, save_path: str, persist_before_save: bool = True
) -> ps.DataFrame:
    """Save the summarized PDF dataframe to a parquet file."""
    if persist_before_save:
        summarized_pdf_df.persist()
    summarized_pdf_df.write.parquet(save_path, mode="overwrite")
    return summarized_pdf_df
