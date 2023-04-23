from helpers import send_email, read_config, get_connection, next_week
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from ast import literal_eval
import os

def get_config_values(config_parser):
    try:
        sender = config_parser.get("gmail_config", "sender")
        email_password = config_parser.get("gmail_config", "password")
        recipients = literal_eval(config_parser.get("gmail_config", "recipients"))
        countries = literal_eval(config_parser.get("query_config", "countries"))
        types = literal_eval(config_parser.get("query_config", "types"))
        database = config_parser.get("sql_config", "database")
        user = config_parser.get("sql_config", "user")
        sql_password = config_parser.get("sql_config", "password")
        host = config_parser.get("sql_config", "host")
        port = config_parser.get("sql_config", "port")
    except Exception as e:
        print(f"Reading configuration failed: {e}")
        raise
    return sender, email_password, recipients, countries, types, database, user, sql_password, host, port

def fetch_data(conn, week, types, countries):
    query_string = text(
            """
            SELECT
                a.date::date,
                a.country,
                a.name,
                b.state,
                b.type
            FROM holiday a
            RIGHT OUTER JOIN holiday_state_type b
            ON a.holiday_id = b.holiday_id
            WHERE EXTRACT('week' FROM date) = :week
            AND
            b.type IN :types
            AND
            a.country IN :countries
            ORDER BY a.date
            """
            )
    try:
        res = conn.execute(query_string, {"week":week, "types":types, "countries":countries})
    except SQLAlchemyError as e:
        print(f"An error occurred while reading data from database: {e}")
        raise
    data = res.fetchall()
    conn.close()
    return data

def create_body(raw_data):
    if len(raw_data)==0:
        body = "No holidays this week"
    else:
        data = pd.DataFrame(raw_data, columns=['date', 'country', 'name', 'state', 'type'])
        body = data.to_string()
    return body

def main():
    config_file = os.environ.get("WH_CONFIG")

    config_parser = read_config(config_file)
    sender, email_password, recipients, countries, types, database, user, sql_password, host, port = get_config_values(config_parser)
    conn = get_connection(user, sql_password, host, port, database)
    week = next_week()
    raw_data = fetch_data(conn, week, types, countries)
    body = create_body(raw_data)
    subject = f"Holidays on {next_week}th week"
    send_email(subject, body, sender, recipients, email_password)

if __name__=="__main__":
    main()