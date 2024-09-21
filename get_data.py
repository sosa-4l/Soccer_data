from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import requests
import json
import pandas as pd
import time
import keys

def pull_from_api():
    url = "https://v3.football.api-sports.io/players"
    
    player_data=[]
    
    params = {
    'season': 2022,
    'league': 39,
    'page': 1
    }

    payload = {}
    headers = keys.ret_key()
    response = requests.request("GET", url, headers=headers, data=payload, params= params)
    data_dict = response.json()
    res = json.dumps(response.json(), indent=4)
    page_max = data_dict['paging']['total'] +1 
    player_data =[]
    
    df = pd.DataFrame()
    
    for pagey in range (1,page_max):
        params['page'] = pagey
        response = requests.request("GET", url, headers=headers, data=payload, params= params)
        res = response.json()
        print(type(res))
        player_data.append(res)
        print(f"new chunk: {pagey}")
        time.sleep(7)
    
    flat_data=[]
    for i in player_data:
        player_no = len(i['response'])
        player_res = i['response']
        for j in player_res:
            flat_dict= {
                "player_id": j['player']['id'] or 0,
                "player_name": j['player']['name'] or 0,
                "first_name": j['player']['firstname'] or 0,
                "last_name": j['player']['lastname'] or 0,
                "player_age": j['player']['age'] or 0,
                "dob": j['player']['birth']['date'] or 0,
                "nationality": j['player']['nationality'] or 0,
                "height": j['player']['height'] or 0,
                "weight": j['player']['weight'] or 0,
                "photo": j['player']['photo'] or 0,
                "player_team_id": j['statistics'][0]['team']['id'] or 0,
                "player_team_name": j['statistics'][0]['team']['name'] or 0,
                "player_team_logo": j['statistics'][0]['team']['logo'] or 0,
                "player_league_id": j['statistics'][0]['league']['id'] or 0,
                "player_league_name": j['statistics'][0]['league']['name'] or 0,
                "player_league_country": j['statistics'][0]['league']['country'] or 0,
                "player_league_logo": j['statistics'][0]['league']['logo'] or 0,
                "player_league_season": j['statistics'][0]['league']['season'] or 0,
                "apps": j['statistics'][0]['games']['appearences'] or 0,
                "starts": j['statistics'][0]['games']['lineups'] or 0,
                "bench": j['statistics'][0]['substitutes']['in'] or 0,
                "minutes": j['statistics'][0]['games']['minutes'] or 0,
                "position": j['statistics'][0]['games']['position'] or 0,
                "rating": j['statistics'][0]['games']['rating'] or 0,
                "shots": j['statistics'][0]['shots']['total'] or 0,
                "on_target": j['statistics'][0]['shots']['on'] or 0,
                "goals_scored": j['statistics'][0]['goals']['total'] or 0,
                "assists": j['statistics'][0]['goals']['assists'] or 0,
                "goals_conceded": j['statistics'][0]['goals']['conceded'] or 0,
                "saves": j['statistics'][0]['goals']['saves'] or 0,
                "total_passes": j['statistics'][0]['passes']['total'] or 0,
                "key_passes": j['statistics'][0]['passes']['key'] or 0,
                "pass_accuracy": j['statistics'][0]['passes']['accuracy'] or 0,
                "total_tackles": j['statistics'][0]['tackles']['total'] or 0,
                "blocks": j['statistics'][0]['tackles']['blocks'] or 0,
                "interceptions": j['statistics'][0]['tackles']['interceptions'] or 0,
                "total_duels": j['statistics'][0]['duels']['total'] or 0,
                "duels_won": j['statistics'][0]['duels']['won'] or 0,
                "dribble_attempts": j['statistics'][0]['dribbles']['attempts'] or 0,
                "successful_dribbles": j['statistics'][0]['dribbles']['success'] or 0,
                "fouls_drawn": j['statistics'][0]['fouls']['drawn'] or 0,
                "fouls_committed": j['statistics'][0]['fouls']['committed'] or 0,
                "red_cards": (j['statistics'][0]['cards']['red'] or 0) + (j['statistics'][0]['cards']['yellowred'] or 0),
                "yellow_cards": j['statistics'][0]['cards']['yellow'] or 0,
                "penalties_won": j['statistics'][0]['penalty']['won'] or 0,
                "penalties_committed": j['statistics'][0]['penalty']['commited'] or 0,
                "penalties_scored": j['statistics'][0]['penalty']['scored'] or 0,
                "penalties_missed": j['statistics'][0]['penalty']['missed'] or 0,
                "penalties_saved": j['statistics'][0]['penalty']['saved'] or 0,
            }
            flat_data.append(flat_dict)
    df = pd.DataFrame(flat_data) 
    df.to_csv('player_stats.csv', index=False)
    print(df)
    return(df)
        
