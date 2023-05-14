import pandas as pd
import json
from google.cloud import storage
from helpers import read_config, next_year, blob_names, get_connection
from sqlalchemy import text
import os
from ast import literal_eval
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import table, column

# Get configuration values from the config file
def get_config_values(config_parser):
    try:
        database = config_parser.get("sql_config", "database")
        user = config_parser.get("sql_config", "user")
        password = config_parser.get("sql_config", "password")
        host = config_parser.get("sql_config", "host")
        port = config_parser.get("sql_config", "port")
        bucket_name = config_parser.get("bucket_config", "bucket_name")
        countries = literal_eval(config_parser.get("extract_config", "countries"))
        years = literal_eval(config_parser.get("extract_config", "years"))
        service_key = config_parser.get("bucket_config", "service_key")
    except Exception as e:
        print(f"Reading configuration failed: {e}")
        raise
    if not years:
        years = next_year()
    return database, user, password, host, port, bucket_name, countries, years, service_key

# Fetch content from Google Cloud Storage bucket
def fetch_content(bucket_name, source_blob):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob)
        content = blob.download_as_string()
        return json.loads(content)
    except Exception as e:
        print(f"Error fetching data from storage: {e}")
        raise

# Get the last index from the HOLIDAY table in the database
def last_index(conn):
    query_string = text("SELECT MAX(HOLIDAY_ID) FROM HOLIDAY")
    try:
        res = conn.execute(query_string)
    except SQLAlchemyError as e:
        print(f"An error occurred while reading holiday index from database: {e}")
        raise
    last_holiday_id = res.fetchall()
    last_holiday_id = last_holiday_id[0][0]
    if last_holiday_id:
        return last_holiday_id + 1
    else:
        return 0

# Convert JSON content to a pandas DataFrame
def json_to_df(content):
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
    return data

# Generate indices for holidays in the DataFrame
def generate_indices(df, first_index):
    indices_dict = {}
    indices_list = []
    top_index = first_index
    for index, row in df.iterrows():
        holiday_date = row['name'] + "_" + row['date']
        try:
            indices_list.append(indices_dict[holiday_date])
        except:
            indices_list.append(top_index)
            indices_dict[holiday_date] = top_index
            top_index += 1
    return indices_list

# Convert a dictionary to a list if it is a dictionary, otherwise return the input unchanged
def dict_to_list(dictionary):
    if isinstance(dictionary, dict):
        return list(dictionary.values())
    else:
        return dictionary

# Construct holiday and holiday_state_type tables from the input DataFrame
def construct_tables(df, indices):
    df['holiday_id'] = indices
    df['country'] = df['country'].apply(dict_to_list)
    df[['country_code','country']] = pd.DataFrame(df['country'].to_list())

    holiday_table = df[['holiday_id','name','description','country','country_code','date']]
    holiday_table = holiday_table.drop_duplicates(subset='holiday_id')

    holiday_state_type_table = df[['holiday_id', 'state', 'type']].explode('state')
    holiday_state_type_table = holiday_state_type_table.explode('type')
    holiday_state_type_table['state'] = holiday_state_type_table['state'].apply(dict_to_list)
    holiday_state_type_table.loc[holiday_state_type_table['state'] == 'All', 'state'] = holiday_state_type_table.loc[holiday_state_type_table['state'] == 'All', 'state'].apply(lambda x: 5 * [x])
    holiday_state_type_table.reset_index(inplace=True, drop=True)
    holiday_state_type_table[['state_num', 'state_abbrev', 'state_name', 'state_type', 'state_id']] = pd.DataFrame(holiday_state_type_table['state'].to_list())
    holiday_state_type_table = holiday_state_type_table[['holiday_id', 'state_name', 'type']]
    holiday_state_type_table.rename(columns={'state_name':'state'}, inplace=True)

    return holiday_table, holiday_state_type_table

# Insert DataFrame content into the specified SQL table, with optional return values
def df_to_sql(df, sql_table, conn, returning_col=None):
    if not df.empty:
        columns = [column(i) for i in df.columns]
        my_table = table(sql_table, *columns)
        insert_stmt = insert(my_table).values(list(df.itertuples(name=None, index=False)))
        if returning_col:
            insert_stmt = insert_stmt.returning(my_table.c[returning_col])
        do_nothing_stmt = insert_stmt.on_conflict_do_nothing()
        try:
            res = conn.execute(do_nothing_stmt)
        except SQLAlchemyError as e:
            raise SQLAlchemyError(f"An error occurred while inserting data: {e}")
        if returning_col:
            return res.fetchall()
    else:
        print("Nothing uploaded. Emtpy DataFrame.")
        return []

# Upload holiday_table and holiday_state_type_table to the database
def upload_to_database(holiday_table, holiday_state_type_table, conn):
    holiday_loaded_ids = df_to_sql(df=holiday_table,
                               sql_table='holiday',
                               conn=conn,
                               returning_col='holiday_id')
    valid_ids = [i[0] for i in holiday_loaded_ids]
    holiday_state_type_table_filtered = holiday_state_type_table[holiday_state_type_table['holiday_id'].isin(valid_ids)]
    holiday_state_type_loaded_ids = df_to_sql(df=holiday_state_type_table_filtered,
                                          sql_table='holiday_state_type',
                                          conn=conn,
                                          returning_col='holiday_id')
    print(f"{len(holiday_loaded_ids)} out of {len(holiday_table)} uploaded to database.")
    print(f"{len(holiday_state_type_loaded_ids)} out of {len(holiday_state_type_table)} uploaded to database.")

# Main function to execute the script
def main():
    config_file = os.environ.get("WH_CONFIG")

    config_parser = read_config(config_file)
    database, user, password, host, port, bucket_name, countries, years, service_key = get_config_values(config_parser)
    if service_key:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_key

    conn = get_connection(user, password, host, port, database)
    blobs = blob_names(countries, years)

    for country, year in blobs:
        source_blob = f"{country}_{year}.json"
        content = fetch_content(bucket_name, source_blob)
        df = json_to_df(content)
        first_index = last_index(conn)
        indices = generate_indices(df, first_index)
        holiday_table, holiday_state_type_table = construct_tables(df, indices)
        upload_to_database(holiday_table, holiday_state_type_table, conn)

    conn.close()

if __name__=="__main__":
    main()


