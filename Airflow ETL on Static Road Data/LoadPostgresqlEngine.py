from sqlalchemy import create_engine	# for Python PostgreSQL engine.
from SimpleLogEngine import log 		# for logging.
import pandas as pd

def postgresql_load(csv, db, table):
	"""
	Loads data from a specified CSV into a specified PostgreSQL database and table.

	Parameters
	__________
	csv : str
		File path to CSV.
	db : str
		PostgreSQL database name to use.
	table : str
		PostgreSQL database table name to upload CSV data into.
	"""

	postgres_user = 'postgres'
	postgres_pswrd = 'pswrd_here'
	localhost = 'localhost_here'

	# create postgresql engine.
	postgresql_engine = create_engine(f'postgresql://{postgres_user}:{postgres_pswrd}@localhost:{localhost}/{db}')

	# create DF out of csv file.
	df = pd.read_csv(csv, index_col=None)

	# load DF into Postgresql database, replacing if exists.
	df.to_sql(name=table, con=postgresql_engine, if_exists='replace', index=False)


	log(f'postgresql_load,{csv} {db} {table},Updated PostgreSQL {db} {table} data,{__file__}')

	print(f'PostgreSQL load complete from file "{csv}" to database "{db}" table "{table}".')