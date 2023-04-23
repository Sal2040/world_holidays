import requests
import json
from google.cloud import storage
import os
from helpers import read_config, next_year, blob_names
from ast import literal_eval

# Get configuration values from the config file
def get_config_values(config_parser):
    try:
        api_key = config_parser.get("request_config", "api_key")
        bucket_name = config_parser.get("bucket_config", "bucket_name")
        service_key = config_parser.get("bucket_config", "service_key")
        countries = literal_eval(config_parser.get("request_config", "countries"))
        years = literal_eval(config_parser.get("request_config", "years"))
    except Exception as e:
        print(f"Reading configuration failed: {e}")
        raise
    if not years:
        years = next_year()
    return api_key, bucket_name, service_key, countries, years

# Fetch holidays data using Calendarific API
def fetch_holidays(api_key, year, country):
    url = f"https://calendarific.com/api/v2/holidays?&api_key={api_key}&country={country}&year={year}"
    try:
        r = requests.get(url)
        r.raise_for_status()
        result =  r.json()
        if not result['response']:
            raise ValueError("No data.")
    except requests.exceptions.RequestException as e:
        print(f"Fetching data failed. Invalid API key or server unavailable: {e}")
        raise
    except ValueError as e:
        print(f"Connection successful but no data fetched. Check 'year' and 'country' arguments: {e}")
        raise
    else:
        print("Holidays fetched.")
        return json.dumps(result)

# Upload fetched holidays data to Google Cloud Storage
def upload_to_storage(content, bucket_name, destination_blob_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(content)
        print("Holidays uploaded to storage")
    except Exception as e:
        print(f"Error uploading to storage: {e}")
        raise

# Main function
def main():
    config_file = os.environ.get("WH_CONFIG")
#    config_file = '/home/sal/PROJEKTY_CV/world_holidays/pipeline.conf'

    config_parser = read_config(config_file)
    api_key, bucket_name, service_key, countries, years = get_config_values(config_parser)

    if service_key:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_key

    blobs = blob_names(countries, years)

    for country, year in blobs:
        destination_blob_name = f"{country}_{year}.json"
        content = fetch_holidays(api_key, year, country)
        upload_to_storage(content, bucket_name, destination_blob_name)

if __name__=="__main__":
    main()