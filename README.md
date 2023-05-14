# World Holidays

This project is designed to fetch holiday data from the Calendarific API, store it in Google Cloud Storage, and send weekly emails with upcoming holidays based on the specified countries and holiday types. The project consists of five Python scripts and an Airflow DAG.

## Requirements
* Python 3.6+
* pip
* PostgreSQL
* Airflow
* Active GCP project
* Email client that provides access to its SMTP server (e.g. Gmail or Yahoo) 

## Local Setup:
1. Clone the repository
```bash
git clone https://github.com/your_username/world_holidays.git
```

2. Install the required packages:
```bash
pip install -r <path_to_your_directory>/world_holidays/requirements.txt
```

3. Start psql as a user of your choice and run the create_database.sql script:
```bash
psql -U <username>
\i <path_to_your_directory>/world_holidays/create_database.sql
```

4. Set up GCS:
* Create a GCS [bucket](https://cloud.google.com/storage/docs/discover-object-storage-console)
* Set up a [service account](https://cloud.google.com/iam/docs/service-accounts-create) with "Storage Admin" role
* Create and download [service account key](https://cloud.google.com/iam/docs/keys-create-delete)

5. Sign up with https://calendarific.com/ in order to get an api key.

6. Acquire SMTP credentials from your email service.
   If you use gmail, use the following:  
   * SSL Port:    465
   * Server Name: smtp.gmail.com
   * Generate application password as shown [here](https://support.google.com/mail/answer/185833?hl=en-GB).
   
7. Set up Airflow following the instructions [here](https://airflow.apache.org/docs/apache-airflow/stable/start.html)

8. Configure
*  Create a pipeline.conf file in the project directory to store your configurations. Use the provided pipeline_empty.conf as a template.
*  Set the WH_CONFIG environment variable to the path of your pipeline.conf file:
```
export WH_CONFIG=path/to/pipeline.conf
```
