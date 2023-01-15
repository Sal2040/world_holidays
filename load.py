import configparser
from google.cloud import storage
import psycopg2
import argparse
import os
import json
from google.oauth2 import service_account
import io

CONFIG_FILE = 'pipeline.conf'

config_parser = configparser.ConfigParser()
config_parser.read(CONFIG_FILE)

# database config
database = config_parser.get("sql_config", "database")
user = config_parser.get("sql_config", "user")
password = config_parser.get("sql_config", "password")
host = config_parser.get("sql_config", "host")
port = config_parser.get("sql_config", "port")


# storage bucket config
bucket_name = config_parser.get("bucket_config", "bucket_name")

# arg_parser = argparse.ArgumentParser()
# arg_parser.add_argument("--source_blob", required=False)
# args = arg_parser.parse_args()
# source_blob_name = args.source_blob

# database connection
conn = psycopg2.connect(database=database,
                        user=user,
                        password=password,
                        host=host,
                        port=port)
conn.autocommit = True

# bucket connection

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/sal/PROJEKTY_CV/world_holidays/worldholidays-370021-b43ad8c40083.json'
# storage_client = storage.Client()

with open('/home/sal/PROJEKTY_CV/world_holidays/worldholidays-370021-b43ad8c40083.json') as source:
    info = json.load(source)

storage_credentials = service_account.Credentials.from_service_account_info(info)
storage_client = storage.Client(project='worldholidays-370021', credentials=storage_credentials)

bucket = storage_client.bucket(bucket_name)
blob = bucket.blob('holiday_table_2022-12-11.csv')

# set stream from blob
file_obj = io.BytesIO()
blob.download_to_file(file_obj)
file_obj.seek(0)

cur = conn.cursor()
cur.execute(
    '''
    CREATE TEMP TABLE tmp_table 
    (LIKE holiday)
    '''
)
cur.copy_expert("COPY tmp_table FROM STDIN WITH CSV", file_obj)
cur.execute(
    '''
    INSERT INTO holiday
    SELECT * FROM tmp_table
    ON CONFLICT DO NOTHING
    '''
)

conn.close()