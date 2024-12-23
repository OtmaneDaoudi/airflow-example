# Overview
This demo aims to showcase the general workflow of creating DAGs in Apache airflow by making a simple ETL data pipeline that extracts a CSV file containing user data, hashs the passwords, and loads the result in an SQLite database.

# Getting Started
1. Create a new Google Cloud project.
2. Open Google Cloud Shell.
3. Clone the repository using `git clone https://github.com/OtmaneDaoudi/airflow-example`
4. `cd airflow-example; chmod +x init.s`
5. Execute the init script to build the docker image and initialize airflow's Metastore: `./init.sh`
6. Run Apache airflow: `docker compose up -d`
7. Access Airflow's webserver from the Google Cloud Shell:

![image](https://github.com/user-attachments/assets/582a4c74-862e-4e87-9274-633c09b510a7)

> [!note]
> Username and Password are both `airflow`

![image](https://github.com/user-attachments/assets/089bea74-bd00-43d1-8379-466b18d97765)

> [!note]
> The Webserer takes about 5 minutes to get up and running.

10. Execute the DAG
11. You should see the extracted and transformed CSVs in the dags folder: `ls ./dags`
12. To ensure the data is loaded into SQLite correctly: `python validate.py`

