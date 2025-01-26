import pandas as pd
import glob
import os

team_mappings = {}
merged_df = []

# Reads in a file
def read_file_as_dict():
    """
    Reads a file containg cities and the team they map in the format key:value
    The function reads each line and stores them in a dictionary.

    Returns:
    None
    """
    with open('teams_mapping.txt', 'r') as file:
        lines = file.readlines()
        for line in lines:
            key, value = line.strip().split(':')
            team_mappings[key] = value

def clean_penalty(penalty_file):
    """
    Cleans the penalty dataframe from a given file.
    Removes last row, and adds columns for team, year, and opponent

    Parameters:
    penalty_file (str): The path to the penalty csv file

    Returns:
    DataFrame: a cleaned DataFrame
    """

    penalty_df = pd.read_csv(penalty_file)

    penalty_df = penalty_df.iloc[:-1]   
    penalty_df['Team'] = os.path.basename(penalty_file).split('_')[0]
    penalty_df['Opponent'] = penalty_df['Opponent'].map(team_mappings)
    penalty_df['Year'] = os.path.basename(penalty_file).split('_')[1].split('.')[0]
    penalty_df.drop(['Outcome', 'Week'], axis=1, inplace=True)
    penalty_df.rename(columns={'Total Count': 'Penalty Count', 'Total Yards': 'Penalty Yards'}, inplace=True)
    penalty_df.reset_index(drop=True, inplace=True)

    return penalty_df

def clean_stats(stats_file):
    """
    Cleans the stats data frame from a given file.
    Removes the first header row, removes empty rows, renames the column, and drops specified columns

    Parameters:
    stats_file (str): The path to the stats csv file

    Returns:
    DataFrame: a cleaned DataFrame
    """

    new_stats_columns = [
    'Week', 'Day', 'Date', 'Time', 'Boxscore', 'Result', 'OT', 'Record', 'Location', 'Opponent',
    'Points Scored', 'Points Allowed', 'Off 1stD', 'Off TotYd', 'Off PassY', 'Off RushY', 'Off To',
    'Def 1stD', 'Def TotYd', 'Def PassY', 'Def RushY', 'Def To', 'Off ExP', 'Def ExP', 'ST ExP'
    ]

    # Read the CSV file with the first two rows as headers
    stats_df = pd.read_csv(stats_file, header=[0, 1])

    # Drop the first header row
    stats_df.columns = stats_df.columns.droplevel(0)

    # Remove rows containing specific strings
    stats_df = stats_df[~stats_df.apply(lambda row: row.astype(str).str.contains("Bye Week|Playoffs|preview|Canceled").any(), axis=1)]

    # Set the new column names
    stats_df.columns = new_stats_columns

    # Drop the specified columns
    stats_df.drop(['Date', 'Boxscore', 'Opponent'], axis=1, inplace=True)
    stats_df.reset_index(drop=True, inplace=True)
    return stats_df

def merge_data():

    all_data = []
    penalty_files = glob.glob("team_weekly_penalty/*.csv")
    read_file_as_dict()

    for penalty_file in penalty_files:

        file_name = os.path.basename(penalty_file)
        stats_file = f"team_weekly_stats/{file_name}"

        penalty_df = clean_penalty(penalty_file).copy()
        stats_df = clean_stats(stats_file).copy()

        if penalty_df.shape[0] != stats_df.shape[0]:
            print(f"WARNING: Row count mismatch for {os.path.basename(penalty_file)}")
            print(penalty_df)
            print(stats_df)

        merged_df = pd.concat([penalty_df, stats_df], axis=1)
        all_data.append(merged_df)
    
    merged_df = pd.concat(all_data, ignore_index=True)
    merged_df.to_csv('merged_data.txt', index=False)

    return merged_df

def clean_merged():

    merged_df = merge_data()

    new_column_order = [
    'Team', 'Opponent', 'Time', 'Day', 'Week', 'Year', 'Date', 'Result', 'OT', 'Location', 'Record',
    'Ref Crew', 'Penalty Count', 'Penalty Yards', 'Off Count', 'Off Yards', 'Def Count', 'Def Yards',
    'ST Count', 'ST Yards', 'Points Scored', 'Points Allowed', 'Off 1stD', 'Off TotYd', 'Off PassY',
    'Off RushY', 'Off To', 'Def 1stD', 'Def TotYd', 'Def PassY', 'Def RushY', 'Def To', 'Off ExP',
    'Def ExP', 'ST ExP'
    ]

    merged_df = merged_df[new_column_order]

    merged_df['Off To'] = merged_df['Off To'].fillna(0).astype(int)
    merged_df['Def To'] = merged_df['Def To'].fillna(0).astype(int)

    merged_df['Result'] = merged_df['Result'].map({'W': 1, 'L': 0, 'T': 0})
    merged_df['OT'] = merged_df['OT'].map({'OT': 1})
    merged_df['Location'] = merged_df['Location'].map({'@': 1})

    merged_df['OT'] = merged_df['OT'].fillna(0).astype(int)
    merged_df['Location'] = merged_df['Location'].fillna(0).astype(int)

    merged_df['Team'] = merged_df['Team'].str.strip().str.lower()
    merged_df['Opponent'] = merged_df['Opponent'].str.strip().str.lower()

    merged_df['Week'] = merged_df['Week'].replace({
        'Wild Card': 19,
        'Division': 20,
        'Conf. Champ.': 21,
        'SuperBowl': 22
    })

    merged_df = merged_df.astype({'Week': 'int', 'Year': 'int', 'OT': 'int', 'Location': 'int', 
                                  'Points Scored': 'int', 'Points Allowed': 'int', 'Off 1stD': 'int', 
                                  'Off TotYd': 'int', 'Off PassY': 'int', 'Off RushY': 'int',
                                  'Def 1stD': 'int', 'Def TotYd': 'int', 'Def PassY': 'int', 
                                  'Def RushY': 'int'})
    
    # Create a unique identifier for each game
    merged_df['Game Key'] = merged_df.apply(lambda row: (row['Year'], row['Week'], tuple(sorted([row['Team'], row['Opponent']]))), axis=1)

    # Assign a unique game ID based on the Game Key
    merged_df['Game ID'] = merged_df.groupby('Game Key').ngroup()

    # Drop the temporary Game Key column
    merged_df.drop(columns=['Game Key'], inplace=True)
    
    merged_df.insert(merged_df.columns.get_loc('Points Allowed')+1, 'Point Differential', merged_df['Points Scored'] - merged_df['Points Allowed'])

    print(merged_df.dtypes)
    merged_df.to_csv('merged_data.txt', index=False)



clean_merged()