import pandas as pd
import sqlalchemy as saly
import psycopg2

# Sample DataFrame
df = pd.DataFrame({
    'name': ['John', 'Alice', 'Bob'],
    'score': [85, 90, 78],
    'team': ['A', 'B', 'A']
})

# PostgreSQL connection string
username = 'postgres'
password = 'admin'
hostname = 'localhost'
port = 5432
database_name = 'Test'

# Create the engine using SQLAlchemy
engine = saly.create_engine(f'postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{database_name}')
connection = engine.connect()
# Write DataFrame to PostgreSQL table (replace 'your_table' with the table name you want)
df.to_sql('your_table', con= connection, if_exists='replace', index=False)

# Closing the connection (optional, as SQLAlchemy handles connection closing automatically)
engine.dispose()
