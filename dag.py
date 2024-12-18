from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import csv
import hashlib
import sqlite3

CSV_URL = r"https://gist.githubusercontent.com/jay0lee/90b17abb4b4bf89df39a025b4540491c/raw/095f2fe66a44a60efc792481798ed3e766b5db25/users.csv"
CWD = "/opt/airflow/dags"

# DB
DB_FILE = f"{CWD}/db.sqlite"
TABLE_NAME = "users"


def transform_csv():
    # Open the input CSV file for reading and the output file for writing
    with open(f"{CWD}/users.csv", mode='r', newline='') as infile, open(f"{CWD}/users_cleaned.csv", mode='w', newline='') as outfile:
        # Initialize CSV reader and writer
        reader = csv.DictReader(infile)  # Read data as dictionaries
        fieldnames = reader.fieldnames  # Get the header from the input file
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)  # Write to output file
        
        # Write the header to the output file
        writer.writeheader()
        
        # Process each row in the CSV file
        for row in reader:
            # Hash the password and replace the plain password with the hashed value
            row['password'] = hashlib.sha256(row['password'].encode('utf-8')).hexdigest()
            
            # Write the updated row to the output file
            writer.writerow(row)

def load_csv():
    # Connect to SQLite database (or create it if it doesn't exist)
    connection = sqlite3.connect(DB_FILE)
    cursor = connection.cursor()

    # Create the table if it doesn't exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        username TEXT PRIMARY KEY,
        firstname TEXT,
        lastname TEXT,
        password TEXT
    );
    """
    cursor.execute(create_table_query)

    # Read the CSV file
    with open(f"{CWD}/users_cleaned.csv", 'r') as file:
        csv_reader = csv.DictReader(file)

        # Validate required columns
        required_columns = {'username', 'firstname', 'lastname', 'password'}
        if not required_columns.issubset(csv_reader.fieldnames):
            raise ValueError(f"CSV file must contain the following columns: {required_columns}")

        # Insert rows into the database
        insert_query = f"""
        INSERT INTO {TABLE_NAME} (username, firstname, lastname, password)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(username) DO UPDATE SET
            firstname = excluded.firstname,
            lastname = excluded.lastname,
            password = excluded.password;
        """
        for row in csv_reader:
            cursor.execute(insert_query, (row['username'], row['firstname'], row['lastname'], row['password']))

    # Commit changes and close the connection
    connection.commit()
    cursor.close()
    connection.close()

    print("Data loaded successfully.")

with DAG(
    dag_id = "ETL_pipeline",
    start_date = datetime.today(),
    catchup = False
) as dag:
    extract = BashOperator(
        task_id = "Extract_raw_csv",
        bash_command = f"curl -o users.csv {CSV_URL}",
        cwd = CWD
    )
    
    transform = PythonOperator(
        task_id = "Hash_passwords",
        python_callable = transform_csv
    )

    load = PythonOperator(
        task_id = "Save_to_mysql",
        python_callable = load_csv
    )
    
    extract >> transform >> load
