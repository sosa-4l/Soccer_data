import sqlalchemy as saly
import psycopg2
import pandas as pd
import get_data
import passs

df = get_data.pull_from_api()

def load_to_psql(df, league_no):
    league_table_name = ['prem_player_stats', 'liga_player_stats']
    
    # PostgreSQL connection string
    username = 'postgres'
    password = passs.passw()
    hostname = 'localhost'
    port = 5432
    database_name = 'Player_stats'

    engine = saly.create_engine(f'postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{database_name}')
    connection = engine.connect()

    df.to_sql(league_table_name[league_no], con= connection, if_exists='replace', index=False)

    engine.dispose()
# df = pd.read_csv('output_nested.csv')

#load_to_psql(df)