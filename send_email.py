from helpers import send_email
import configparser

def main():
    CONFIG_FILE = 'pipeline.conf'

    config_parser = configparser.ConfigParser()
    config_parser.read(CONFIG_FILE)
    sender = config_parser.get("gmail_config", "sender")
    password = config_parser.get("gmail_config", "password")
    recipients = config_parser.get("gmail_config", "recipients")

    subject = "Email Subject"
    body = "This is the body of the text message"
    sender = sender
    recipients = eval(recipients)
    password = password
    send_email(subject, body, sender, recipients, password)

if __name__=="__main__":
    main()