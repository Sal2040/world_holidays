import pandas as pd
import json
import configparser
from google.cloud import storage
import helpers
from sqlalchemy import create_engine
import psycopg2

CONFIG_FILE = 'pipeline.conf'

config_parser = configparser.ConfigParser()
config_parser.read(CONFIG_FILE)
database = config_parser.get("sql_config", "database")
user = config_parser.get("sql_config", "user")
password = config_parser.get("sql_config", "password")
host = config_parser.get("sql_config", "host")
port = config_parser.get("sql_config", "port")

conn_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
db = create_engine(conn_string)
conn = db.connect()

content = load_json

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

last_holiday_id = ...

data['holiday_id'] = range(last_holiday_id, len(data) + last_holiday_id)

data['country'] = data['country'].apply(helpers.dict_to_list)
data[['country_id','country']] = pd.DataFrame(data['country'].to_list())

holiday_table = data[['holiday_id','name','description','country','date']]

holiday_type_table = data[['holiday_id','type']].explode('type')

holiday_location_table = data[['holiday_id','location']].explode('location')

holiday_state_table = data[['holiday_id','state']].explode('state')
holiday_state_table['state'] = holiday_state_table['state'].apply(helpers.dict_to_list)
holiday_state_table.loc[holiday_state_table['state']=='All','state'] = holiday_state_table.loc[holiday_state_table['state']=='All','state'].apply(lambda x: 5*[x])
holiday_state_table[['state_num','state_abbrev','state_name','state_type','state_id']] = pd.DataFrame(holiday_state_table['state'].to_list())
holiday_state_table = holiday_state_table[['holiday_id','state_name']]



