import pandas as pd
import json
import configparser
from google.cloud import storage
from helpers import dict_to_list
from sqlalchemy import create_engine, text
import psycopg2
import argparse
import datetime as dt

def main():
    CONFIG_FILE = 'pipeline.conf'

    config_parser = configparser.ConfigParser()
    config_parser.read(CONFIG_FILE)
    database = config_parser.get("sql_config", "database")
    user = config_parser.get("sql_config", "user")
    password = config_parser.get("sql_config", "password")
    host = config_parser.get("sql_config", "host")
    port = config_parser.get("sql_config", "port")
    bucket_name = config_parser.get("bucket_config", "bucket_name")

    arg_parser = argparse.ArgumentParser()
    default_blob = dt.datetime.now().strftime("%Y-%m-%d") + ".json"
    arg_parser.add_argument("--source_blob", default=f"{default_blob}", required=True)
    args = arg_parser.parse_args()
    source_blob_name = args.source_blob

    conn_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
    db = create_engine(conn_string)
    conn = db.connect()

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    content = blob.download_as_string()
    content = json.loads(content)

    query_string = text("SELECT MAX(HOLIDAY_ID) FROM HOLIDAY")
    res = conn.execute(query_string)
    last_holiday_id = res.fetchall()
    last_holiday_id = last_holiday_id[0][0]
    if last_holiday_id:
        start_holiday_id = last_holiday_id + 1
    else:
        start_holiday_id = 0

    name = []
    description = []
    country = []
    date = []
    type_ = []
    location = []
    state = []

    for holiday in content['response']['holidays']:
        name.append(holiday['name'])
        description.append(holiday['description'])
        country.append(holiday['country'])
        date.append(holiday['date']['iso'])
        type_.append(holiday['type'])
        location.append(holiday['locations'])
        state.append(holiday['states'])

    data = pd.DataFrame({
        'name': name,
        'description': description,
        'country': country,
        'date': date,
        'type': type_,
        'location': location,
        'state': state
    })

    indices_dict = {}
    indices_list = []
    top_index = start_holiday_id
    for index, row in data.iterrows():
        holiday_date = row['name'] + "_" + row['date']
        try:
            indices_list.append(indices_dict[holiday_date])
        except:
            indices_list.append(top_index)
            indices_dict[holiday_date] = top_index
            top_index += 1

    data['holiday_id'] = indices_list

    data['country'] = data['country'].apply(dict_to_list)
    data[['country_id','country']] = pd.DataFrame(data['country'].to_list())

    holiday_table = data[['holiday_id','name','description','country','date']]
    holiday_table = holiday_table.drop_duplicates(subset='holiday_id')

    holiday_state_type_table = data[['holiday_id', 'state', 'type']].explode('state')
    holiday_state_type_table = holiday_state_type_table.explode('type')
    holiday_state_type_table['state'] = holiday_state_type_table['state'].apply(dict_to_list)
    holiday_state_type_table.loc[holiday_state_type_table['state'] == 'All', 'state'] = holiday_state_type_table.loc[holiday_state_type_table['state'] == 'All', 'state'].apply(lambda x: 5 * [x])
    holiday_state_type_table.reset_index(inplace=True, drop=True)
    holiday_state_type_table[['state_num', 'state_abbrev', 'state_name', 'state_type', 'state_id']] = pd.DataFrame(holiday_state_type_table['state'].to_list())
    holiday_state_type_table = holiday_state_type_table[['holiday_id', 'state_name', 'type']]
    holiday_state_type_table.rename(columns={'state_name':'state'}, inplace=True)

    holiday_table.to_sql('holiday', con=conn, index=False, if_exists='append')
    holiday_state_type_table.to_sql('holiday_state_type', con=conn, index=False, if_exists='append')

    conn.close()

if __name__=="__main__":
    main()


