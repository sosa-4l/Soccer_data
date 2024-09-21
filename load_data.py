import sqlalchemy as saly
import psycopg2
import pandas as pd
import get_data
import passs

#get_data.pull_from_api()

def load_to_psql(df):

    # PostgreSQL connection string
    username = 'postgres'
    password = passs.passw()
    hostname = 'localhost'
    port = 5432
    database_name = 'Player_stats'

    # Create the engine using SQLAlchemy
    engine = saly.create_engine(f'postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{database_name}')
    connection = engine.connect()
    # Write DataFrame to PostgreSQL table 
    df.to_sql('prem_player_stats', con= connection, if_exists='replace', index=False)

    # Closing the connection (optional, as SQLAlchemy handles connection closing automatically)
    engine.dispose()
df = pd.read_csv('output_nested.csv')
load_to_psql(df)