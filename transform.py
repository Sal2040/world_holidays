import pandas as pd
import configparser
import datetime as dt
import argparse
import json
from google.cloud import storage

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
    date.append(holiday['date']['datetime'])
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
