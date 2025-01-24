import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO

teams_file = open('teams_codes.txt', 'r')
teams = teams_file.read().split('\n')

team_namea_file = open('teams.txt', 'r')
team_names = team_namea_file.read().split('\n')

for year in range(2010,2025):

    i = 0

    for team in teams:
        success = False

        while not success:
            url = f'https://www.pro-football-reference.com/teams/{team}/{year}.htm'
            print(f'{team}: {year}')

            try:
                response = requests.get(url)
                soup = BeautifulSoup(response.text, 'html.parser')

                table = soup.find("table", {"id": "games"})

                data = pd.read_html(StringIO(str(table)))[0]
                data.to_csv(f'team_weekly_stats/{team_names[i]}_{year}.csv', index=False)

                i += 1
                success = True
            except:
                print("Retrying in 5 seconds...")
                time.sleep(5)