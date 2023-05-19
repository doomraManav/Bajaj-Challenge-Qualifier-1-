import os
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine

# Define the MySQL connection details
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = 'md4069'
mysql_database = 'bajaj'

# Define the directory path where the parquet files are located
directory_path = r'C:\Users\doomr\Coding\Bajaj Finserve Qualifier 1'

# Create the SQLAlchemy engine
engine = create_engine(f'mysql://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}')

# Function to create MySQL tables
def create_mysql_tables(file_path):
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    df = pd.read_parquet(file_path)
    df.to_sql(file_name, con=engine, if_exists='replace', index=False)

# Function to load data into MySQL tables
def load_data_to_mysql(file_path):
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    df = pd.read_parquet(file_path)
    df.to_sql(file_name, con=engine, if_exists='replace', index=False)

# Iterate over the parquet files in the directory
for file_name in os.listdir(directory_path):
    if file_name.endswith('.parquet'):
        file_path = os.path.join(directory_path, file_name)

        # Task I: Read the files and identify the schema
        df = pd.read_parquet(file_path)
        print(f'Schema of {file_name}:')
        print(df.dtypes)
        print('-------------------------')

        # Task II: Create MySQL tables
        create_mysql_tables(file_path)
        print(f'Table {file_name} created successfully.')

        # Task III: Load data into MySQL tables
        load_data_to_mysql(file_path)
        print(f'Data loaded into {file_name} table.')

# Close the SQLAlchemy engine
engine.dispose()
