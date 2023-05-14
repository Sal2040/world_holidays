from helpers import read_config, get_connection
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from ast import literal_eval
import os
import datetime as dt
import smtplib
from email.mime.text import MIMEText


# Get configuration values from the config file
def get_config_values(config_parser):
    try:
        sender = config_parser.get("smtp_config", "sender")
        email_password = config_parser.get("smtp_config", "password")
        recipients = literal_eval(config_parser.get("email_config", "recipients"))
        countries = literal_eval(config_parser.get("email_config", "countries"))
        if not countries:
            countries = literal_eval(config_parser.get("extract_config", "countries"))
        countries = tuple(countries)
        types = literal_eval(config_parser.get("email_config", "types"))
        types = tuple(types)
        database = config_parser.get("sql_config", "database")
        user = config_parser.get("sql_config", "user")
        sql_password = config_parser.get("sql_config", "password")
        host = config_parser.get("sql_config", "host")
        sql_port = config_parser.get("sql_config", "port")
        smtp_port = config_parser.get("smtp_config", "port")
        smtp_server = config_parser.get("smtp_config", "server")
    except Exception as e:
        print(f"Reading configuration failed: {e}")
        raise
    return sender, email_password, recipients, countries, types, database, user, sql_password, host, sql_port, smtp_port, smtp_server

# Calculate the next week's number
def next_week():
    return dt.datetime.now().isocalendar()[1]+1

# Fetch data for specified week, types, and countries from the database
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
            a.country_code IN :countries
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

# Create the email body using fetched data
def create_body(raw_data):
    if len(raw_data)==0:
        body = "No holidays this week"
    else:
        data = pd.DataFrame(raw_data, columns=['date', 'country', 'name', 'state', 'type'])
        body = data.to_string()
    return body

# Send email with the given subject and body
def send_email(subject, body, sender, recipients, password, smtp_port, smtp_server):
    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = ', '.join(recipients)
        smtp_server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        smtp_server.login(sender, password)
        smtp_server.sendmail(sender, recipients, msg.as_string())
        smtp_server.quit()
        print("Email sent.")
    except smtplib.SMTPException as e:
        print(f"An error occurred while sending the email: {e}")
        raise

# Main function to execute the script
def main():
    config_file = os.environ.get("WH_CONFIG")

    config_parser = read_config(config_file)
    sender, email_password, recipients, countries, types, database, user, sql_password, host, sql_port, smtp_port, smtp_server = get_config_values(config_parser)
    conn = get_connection(user, sql_password, host, sql_port, database)
    week = next_week()
    raw_data = fetch_data(conn, week, types, countries)
    body = create_body(raw_data)
    subject = f"Holidays on {week}th week"
    send_email(subject, body, sender, recipients, email_password, smtp_port, smtp_server)

if __name__=="__main__":
    main()