from LoadPostgresqlEngine import postgresql_load as pl

# Variables:
csv_path = '/home/project/airflow/dags/trafficproject/staging/transformed_extracted_data.csv'
sql_db_name = 'road_traffic_data'
sql_table_name = 'toll_data'

# Execution:

pl(csv_path, sql_db_name, sql_table_name) # loads the specified CSV file into the specified PostgreSQL database & table name.