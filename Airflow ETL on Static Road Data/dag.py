# imports
from datetime import timedelta 
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago 


# staging variables:
tgz_file_path = '/home/project/airflow/dags/trafficproject/staging/tolldata.tgz'
tgz_directory = '/home/project/airflow/dags/trafficproject/staging/'

# for staging files cleanup destination.
archive_directory = '/home/project/airflow/dags/trafficproject/staging/archive'

# original tgz data source file link.
wget_download_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'

# for creating postgres db if not exists.
postgres_db_name = 'road_traffic_data'
postgres_user = 'usr_here'
postgres_host = 'localhost_here'



# DAG's default args definition.
default_args_variable = {
    'owner': 'BJ Tan',
    'start_date': days_ago(0),
    'email': ['sample@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG creation.
dag = DAG(
    dag_id = 'ETL_toll_traffic_data',
    default_args = default_args_variable,
    description = 'Consolidate road traffic data from CSV, TSV, and fixed-width files.',
    schedule_interval = timedelta(days=1),
)



# DAG tasks:


# downloads compressed tgz file.
download_raw_data = BashOperator(
    task_id = 'download_raw_data',
    bash_command = f'wget -P {tgz_directory} {wget_download_url}',
    dag = dag
    )

# extracts files from compressed tgz file into tgz file's directory.
unzip_raw_data = BashOperator(
    task_id = 'unzip_raw_data',
    bash_command = f'tar -xzvf {tgz_file_path} -C {tgz_directory}',
    dag = dag
)

# extracts row ID (unique and consistent across all 3 files), timestamp, anonymized vehicle number, and vehicle type from vehicle-data.csv and outputs to csv.
csv_output_filename = 'csv_data' + '.csv'
extract_from_csv = BashOperator(
    task_id = 'extract_from_csv',
    bash_command = f'cut -d "," -f 1-4 {tgz_directory}vehicle-data.csv > {csv_output_filename}',
    dag = dag
)

# extracts number of axles, tollplaza id, and tollplaza code from tollplaza-data.tsv and outputs to csv.
tsv_output_filename = 'tsv_data' + '.csv'
extract_from_tsv = BashOperator(
    task_id = 'extract_from_tsv',
    bash_command = f'cut -f 5-7 --output-delimiter="," {tgz_directory}tollplaza-data.tsv > {tsv_output_filename}',
    dag = dag
)

# extracts type of payment code and vehicle code from payment-data.txt and outputs to csv.
fixed_width_output_filename = 'fixed_width_data' + '.csv'
extract_from_fixed_width = BashOperator(
    task_id = 'extract_from_fixed_width',
    bash_command = f'cat {tgz_directory}payment-data.txt | tr -s " " "," | cut -d "," -f 11-12 > {fixed_width_output_filename}',
    dag = dag
)

# combines all extracted csv files' rows horizontally and outputs to single csv, in field order of:
# Row ID, Timestamp, Anonymized Vehicle Number, Vehicle Type, Number of Axles, Type of Payment Code, Vehicle Code, Tollplaza ID, and Tollplaza Code.
extracted_output_filename = 'extracted_data' + '.csv'
consolidation = BashOperator(
    task_id = 'consolidation',
    bash_command = f'paste -d "," {tgz_directory}{csv_output_filename} {tgz_directory}{fixed_width_output_filename} {tgz_directory}{tsv_output_filename} > {extracted_output_filename}',
    dag = dag
)

# transform extracted data.
transformed_extracted_output_filename = 'transformed_extracted_data.csv' # name pulled from transform-extracted-data.py script
transformations = BashOperator(
    task_id = 'transformations',
    bash_command= f'python3 {tgz_directory}transform.py',
    dag = dag
    )

# create DB in PostgreSQL if not exists.
db_create = BashOperator(
    task_id = 'db_create',
    bash_command = f'echo "SELECT \'CREATE DATABASE {postgres_db_name}\' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = \'{postgres_db_name}\')\\gexec" | psql --username={postgres_user} --host={postgres_host}',
    dag = dag
)

# load data into postgresql table.
load = BashOperator(
    task_id = 'load',
    bash_command= f'python3 {tgz_directory}load.py',
    dag = dag
    )

# cleanup staging files by moving most files to archive folder.
stagingcleanup = BashOperator(
    task_id = 'stagingcleanup',
    bash_command= f'mv {tgz_directory}vehicle-data.csv {archive_directory} && mv {tgz_directory}fileformats.txt {archive_directory} && mv {tgz_directory}payment-data.txt {archive_directory} && mv {tgz_directory}{csv_output_filename} {archive_directory} && mv {tgz_directory}{fixed_width_output_filename} {archive_directory} && mv {tgz_directory}tolldata.tgz {archive_directory} && mv {tgz_directory}{extracted_output_filename} {archive_directory} && mv {tgz_directory}tollplaza-data.tsv {archive_directory} && mv {tgz_directory}{tsv_output_filename} {archive_directory}',
    dag = dag
    )


# task pipeline:
download_raw_data >> unzip_raw_data >> extract_from_csv >> extract_from_tsv >> extract_from_fixed_width >> consolidation >> transformations >> db_create >> load >> stagingcleanup