import data_collection
import weaviate

from hamilton import base, driver


def main():
    weaviate_client = weaviate.Client("http://localhost:8080")

    config = dict(
        weaviate_client=weaviate_client,
        rss_url_file_path="./rss_sources.txt",
        overwrite_schema=False,
    )
    dr = driver.Driver(
        config, data_collection, adapter=base.SimplePythonGraphAdapter(base.DictResult())
    )

    outputs = dr.execute(final_vars=["create_weaviate_schema", "html_chunks", "push_to_weaviate"])

    print(outputs.keys())


if __name__ == "__main__":
    main()
