# World Holidays

This project is designed to fetch holiday data from the Calendarific API and send weekly emails with upcoming holidays based on the specified countries and holiday types.  
It uses Google Cloud Storage (GCS) for intermediate storage and a PostgreSQL database for permanent storage of transformed and normalized data. It also uses Airflow DAGs for orchestration. The project is intended to be run either locally or on the [Google Cloud Platforfm (GCP)](https://cloud.google.com/gcp?utm_source=google&utm_medium=cpc&utm_campaign=emea-cz-all-cs-bkws-all-all-trial-e-gcp-1011340&utm_content=text-ad-none-any-DEV_c-CRE_548660727555-ADGP_Hybrid+%7C+BKWS+-+EXA+%7C+Txt+~+GCP+~+General%23v2-KWID_43700060393213358-aud-606988877894:kwd-26415313501-userloc_9062851&utm_term=KW_google+cloud+platform-NET_g-PLAC_&&gad=1&gclid=CjwKCAjwpayjBhAnEiwA-7ena56fLx2D2_om6IAJOBTECWAKDfPhWzi7O3QWmXk-SE7aeqLKtxTUphoCS60QAvD_BwE&gclsrc=aw.ds&hl=en).  

Scripts:
* **extract.py** - extracts data from the Calendarific API and stores them in GCS
* **transform_load.py** - transforms the extracted data into a normalized tabular form and uploads them into the database
* **send_email.py** - fethces data from the database, formats them as text and sends them in via email

DAGs:
* **dag_extract_load.py** - runs **extract.py** and **transform_load.py** sequientially every December 27 at noon in order to fetch holidays data for the upcoming year.
* **dag_send_email.py** - runs **send_emai.py** every Friday at noon.

Module:
* **helpers.py** - aggregates some basic reusable functions

Configuration:
* **pipeline_empty.conf** is provided as a template for creating the configuration file.
* **requirements.txt** contains Python dependencies.

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
* Create a [GCS bucket](https://cloud.google.com/storage/docs/discover-object-storage-console)
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
*  Set the WH_HOME environment variable to the path of your project directory:
```bash
export WH_HOME=<path_to_your_directory>/world_holidays
```

9. Run Airflow standalone.
```bash
airflow standalone
```

## Cloud Setup:
For cloud-only deployment on the GCP, the local instances of PostgreSQL and Airflow need to be replaced with their cloud equivalents.

1. PostgeSQL
   * create a [Cloud SQL instance](https://cloud.google.com/sql/docs/postgres/create-instance)
   * make sure to configure the instance with [private service access](https://cloud.google.com/vpc/docs/private-services-access) so you can connect to it from within your GCP project via [private ip](https://cloud.google.com/sql/docs/postgres/private-ip). This way, the host and port in the `pipeline.conf` can simply be replaced with their cloud equivalents.
   * Install and run [Cloud SQL Auth proxy](https://cloud.google.com/sql/docs/postgres/connect-instance-auth-proxy) and follow step 3. from the Local Setup above.
  
2. Airflow  

   There are two options for setting up a cloud instance of Airflow:
   * [Google Cloud Composer](https://cloud.google.com/composer/docs) - this solution has the advantage of being a fully preinstalled and production-ready. Nevertheless, [dependency conflicts](https://cloud.google.com/composer/docs/composer-2/troubleshooting-package-installation) are almost certain to appear and the solution is not trivial. It is also not possible to simply turn the instance off and on to save costs when it is not needed. It has to be completely deleted and set up again later.
   * Manually installing Airflow on [Virtual Machine](https://cloud.google.com/compute/docs). Whereas this solutions is not suitable for serious production applications, it is good enough for a dummy project. In this case, the steps outlined in the Local Setup above can be simply repeated on the Virtual Machine.  

## Notes on Configuration:
The `pipeline.conf` file has two separate sections to define the countries:
* `[extract_config]` defines the holidays in the countries that are downloaded from the **calendarific API** into the database. Modification requires a manual re-run of the **extract_load DAG** to update the database. Country removals are not reflected. Only additions.
* `[email_config]` defines the holidays in the countries that will be included in the information email. Countries not included in the database cannot be included in the emails of course.

There are the following holiday types to choose from:
 * **National holiday = bank holiday**
 * **Common local holiday = bank holiday**
 * Half-day holiday
 * Local holiday
 * Muslim
 * Hebrew
 * Clock change/Daylight Saving Time
 * Season
 * Christian
 * Observance

The **years** config in the `[extract_config]` section is meant for initial population of the database. If left empty `[]`, it defaults to the next year.

