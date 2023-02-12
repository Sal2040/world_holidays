from helpers import send_email
import configparser
import psycopg2
import pandas as pd
import datetime as dt

def main():
    #get configurations
    CONFIG_FILE = 'pipeline.conf'

    config_parser = configparser.ConfigParser()
    config_parser.read(CONFIG_FILE)

    #email
    sender = config_parser.get("gmail_config", "sender")
    email_password = config_parser.get("gmail_config", "password")
    recipients = eval(config_parser.get("gmail_config", "recipients"))

    #database query
    countries = eval(config_parser.get("query_config", "countries"))
    types = eval(config_parser.get("query_config", "types"))

    #database config
    database = config_parser.get("sql_config", "database")
    user = config_parser.get("sql_config", "user")
    sql_password = config_parser.get("sql_config", "password")
    host = config_parser.get("sql_config", "host")
    port = config_parser.get("sql_config", "port")

    conn = psycopg2.connect(
        database=database,
        user=user,
        password=sql_password,
        host=host,
        port=port
    )
    conn.autocommit = True
    cur = conn.cursor()

    next_week = dt.datetime.now().isocalendar()[1]+1
    sql = """
            SELECT
                a.date::date,
                a.country,
                a.name,
                b.state,
                b.type
            FROM holiday a
            RIGHT OUTER JOIN holiday_state_type b
            ON a.holiday_id = b.holiday_id
            WHERE EXTRACT('week' FROM date) = %s
            AND
            b.type IN %s
            AND
            a.country IN %s
            ORDER BY a.date
            """
    cur.execute(sql, (next_week, types, countries))
    res = cur.fetchall()
    conn.close()

    if len(res)==0:
        body = "No holidays this week"
    else:
        data = pd.DataFrame(res, columns=['date', 'country', 'name', 'state', 'type'])
        body = data.to_string()

    subject = f"Holidays on {next_week}th week"
    sender = sender
    recipients = recipients
    password = email_password
    send_email(subject, body, sender, recipients, password)

if __name__=="__main__":
    main()