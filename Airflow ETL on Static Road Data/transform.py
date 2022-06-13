from TransformationsEngine import Transformer

# Variables
directory = '/home/project/airflow/dags/trafficproject/staging/'
csv_file = 'extracted_data.csv'

# for converting payments column to different currency if other than 'USD' (e.g. 'CNY', 'JPY', etc).
currency_region_abbreviation = 'GBP'


# Execution:
transform = Transformer(directory + csv_file) # creates & stores pandas DF in class object using specified CSV file path.
transform.add_random_usd_payments() # creates the randomly-generated payments column in DF.
transform.currency_convert(currency_region_abbreviation) # converts to different currency; omit if not changing from USD.
transform.formatting() # splits up timestamp column into multiple date columns and re-orders the columns.
transform.get_csv(directory) # exports the DF in the object as a CSV to the specified directory.