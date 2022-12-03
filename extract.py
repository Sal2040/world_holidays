import requests
import configparser
import datetime as dt
import argparse
import json
from google.cloud import storage

def main():
    CONFIG_FILE = 'pipeline.conf'

    config_parser = configparser.ConfigParser()
    config_parser.read(CONFIG_FILE)
    api_key = config_parser.get("request_config", "api_key")
    bucket_name = config_parser.get("bucket_config", "bucket_name")

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--country", default="", required=True)
    arg_parser.add_argument("--year", default="", required=True)
    arg_parser.add_argument("--month", default="")
    arg_parser.add_argument("--day", default="")

    args = arg_parser.parse_args()
    country, year, month, day = args.country, args.year, args.month, args.day

    destination_blob_name = dt.datetime.now().strftime("%Y-%m-%d") + ".json"

    r = requests.get(f"https://calendarific.com/api/v2/holidays?&api_key={api_key}&country={country}&year={year}&month={month}&day={day}")
    contents = json.dumps(r.json())

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(contents)

    print(
        f"{destination_blob_name} uploaded to {bucket_name}."
    )

if __name__=="__main__":
    main()