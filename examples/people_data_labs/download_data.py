import boto3


def main():
    """Simple script to download data files locally"""

    BUCKET = "hamilton-pdl-demo"

    s3 = boto3.client("s3")

    print("Starting data download. It should take a total of ~3min.")
    print("Downloading `pdl_data.json`")
    s3.download_file(Bucket=BUCKET, Key="pdl_data.json", Filename="data/pdl_data.json")
    print("Downloading `stock_data.json`")
    s3.download_file(Bucket=BUCKET, Key="stock_data.json", Filename="data/stock_data.json")

    print("Data successfully downloaded.")


if __name__ == "__main__":
    main()
