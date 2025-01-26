import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO

teams_file = open("teams.txt", 'r')
teams = teams_file.read().split('\n')

for year in range(2009,2010):

    for team in teams:
        
        url = f'https://www.nflpenalties.com/team/{team}?year={year}'

        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        table = soup.find('table')

        data = pd.read_html(StringIO(str(table)))[0]

        data.to_csv(f'team_weekly_penalty/{team}_{year}.csv', index=False)