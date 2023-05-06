import configparser
import datetime as dt
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine

def read_config(config_file):
    config_parser = configparser.ConfigParser()
    config_parser.read(config_file)
    return config_parser

def next_year():
    return [dt.datetime.now().isocalendar()[0] + 1]

def blob_names(countries, years):
    try:
        for country in countries:
            for year in years:
                yield country, year
    except TypeError as e:
        print(f"Wrong input - check config. Years and countries must be in '[]': {e}")
        raise

def get_connection(user, password, host, port, database):
    conn_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
    try:
        db = create_engine(conn_string)
        return db.connect()
    except OperationalError as e:
        print(f"Connection to database failed. Check config: {e}")
        raise