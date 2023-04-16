from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import table, column
import smtplib
from email.mime.text import MIMEText
import configparser
import datetime as dt
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy import create_engine
import psycopg2

def dict_to_list(dictionary):
    if isinstance(dictionary, dict):
        return list(dictionary.values())
    else:
        return dictionary

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

def send_email(subject, body, sender, recipients, password):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    smtp_server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    smtp_server.login(sender, password)
    smtp_server.sendmail(sender, recipients, msg.as_string())
    smtp_server.quit()

def read_config(config_file):
    config_parser = configparser.ConfigParser()
    config_parser.read(config_file)
    return config_parser

def next_year():
    return [dt.datetime.now().isocalendar()[0] + 1]

def next_week():
    return dt.datetime.now().isocalendar()[1]+1

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