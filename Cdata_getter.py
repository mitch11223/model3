from nba_api.stats.endpoints import playergamelog
from nba_api.stats.endpoints import leaguestandings
from nba_api.stats.endpoints import teamestimatedmetrics
from nba_api.stats.endpoints import boxscoreadvancedv3
from nba_api.stats.endpoints import commonplayerinfo
from nba_api.stats.endpoints import boxscoredefensivev2
from nba_api.stats.endpoints import boxscorehustlev2
from nba_api.stats.endpoints import boxscoremiscv3
from nba_api.stats.endpoints import boxscoreplayertrackv3
from nba_api.stats.endpoints import boxscoreusagev3
from nba_api.stats.endpoints import boxscorescoringv3
from nba_api.stats.endpoints import teamgamelog
from nba_api.stats.endpoints import leaguegamelog
from nba_api.stats.endpoints import boxscorematchupsv3
from nba_api.live.nba.endpoints import scoreboard

from json.decoder import JSONDecodeError
from requests.exceptions import ReadTimeout
from sklearn.preprocessing import StandardScaler
import numpy as np
import glob
import requests
import json
import re
import os
import time
import json
import pandas as pd
import fitz 
from scipy.stats import zscore
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

'''
Three runs
1.
gamelogs

2.
Just
append averages to dict \ savematchups

3.
matchup meta to gamelog append
'''
class DataGetter:
    def __init__(self,execution='normal_run'):
            
            if execution == 'normal_run':
                self.date = time.strftime('%m-%d-%Y')
                self.script_directory = os.path.dirname(os.path.abspath(__file__))

                #METADATA
                
                self.players = self.read_players()
                self.rosters = self.read_rosters()
                self.teams = self.read_teams()
                self.game_ids = self.read_gameids()
                self.todays_games = self.get_todays_games()
                self.season = '2023-24'
                self.get_injuries()
            
            
            
            if execution == 'run1':                
                self.date = time.strftime('%m-%d-%Y')
                self.season = '2023-24'
                self.game_ids = self.get_gameids()
                self.players = self.read_players()
                self.rosters = self.read_rosters()
                self.teams = self.read_teams()
                
                #GAMELOGS
                self.get_data(player_info_meta = True) #fetches original gamelogs and saves to dir || set player_info_meta to be True for each season
                                  #creates and updates the player_info dict

                #AVERAGES
                self.home_averages_output_folder = 'players/averages/home/'
                self.away_averages_output_folder = 'players/averages/away/'
                self.combined_averages_output_folder = 'players/averages/combined/'
                self.calc_save_averages()              
                #DVPOS
                self.api_base_url = "https://api.fantasypros.com/v2/json/nba/team-stats-allowed/"
                self.timeframes = ['7', '15', '30']
                self.acquire_dvpos()
                #MEDIANS
                self.acquire_medians()
                #STANDARD DEVIATION
                self.iterate_through_std()
                #TEAM_METRICS
                self.combine_team_dfs()
                #PLAYER AND TEAM METRIC MERGER
                self.merge_teamplayer_data()
                
                #NBA API
                self.accessNBA_API_boxscores()

                #Update Player Game Logs
                self.add_boxscoregamelog()
                
                #PROPS
                #self.props_filename = f"props/{self.date}.txt"
                #self.prop_api_key = 'T0RyrHY6WpXU4FYcIGthiwbtBHe0VUHgIdc2VyDO3g'
                #self.prop_markets = ["player_points_over_under", "player_rebounds_over_under", "player_assists_over_under"]
                #self.prop_data = []
                #self.acquire_props(force_update = False)
                
                #TEAM STATS 
                self.get_teamStats()
                self.add_team_boxscoregamelog()
                
                
            elif execution == 'run2':
                print('\nrun2\n append_averages_to_dict')
                self.date = time.strftime('%m-%d-%Y')
                self.players = self.read_players()
                self.rosters = self.read_rosters()
                self.teams = self.read_teams()
                self.game_ids = self.read_gameids()
                self.season = '2023-24'
                self.append_averages_todict()
                self.processTeamMeta()
                self.saveMatchups(mode='offense')
                self.saveMatchups(mode='defense')
            elif execution == 'run3':
                print('\nrun3\n')
                self.date = time.strftime('%m-%d-%Y')
                self.players = self.read_players()
                self.rosters = self.read_rosters()
                self.game_ids = self.read_gameids()
                self.teams = self.read_teams()
                self.todays_games = self.get_todays_games()
                self.season = '2023-24'
                self.get_injuries()
                self.apply_matchups_meta_gamelogs()
                
                
              
            
            
            #self.fantasy_df_creation()
            #self.todays_lineups = self.gameAnalysis()
                
            


                
    '''
    START
    '''
    
    def apply_matchups_meta_gamelogs(self):
        #GAMELOGV2
        #for each game, this creates metadata for the player matchups
        #appends on the same game in gamelog
        for player_name in self.players:
            player_gamelog = pd.read_csv(f'players/gamelogs/{player_name}_log.csv')
            offense_matchups = pd.read_csv(f'players/matchups/data/offense/{player_name}_matchups.csv')   
            defense_matchups = pd.read_csv(f'players/matchups/data/defense/{player_name}_matchups.csv')
            defense_matchups = defense_matchups.groupby('Game_Id')
            offense_matchups = offense_matchups.groupby('Game_Id')
            
            
            
            #weighted everything for opp attr per game
            
            
            
            
            for game_id, matchups in defense_matchups:
            
                #for each game in player game logs create a column and adda  value for :
                #defender quality/quality of competition (weighted) --Do they play a lot vs bench players? Starters? how do they perform?
                #-weighted average of offensive quality, need to create a dfensive qiality rating per play, then apply it to every one of their matchups
                
                
                #defender height, weight, lbs/cm (weighted)               
                offender_size = (matchups['Player Weight'] / matchups['Player Height']) * matchups['partialPossessions']
                offender_size = offender_size.sum() / matchups['partialPossessions'].sum()
                offender_height = (matchups['Player Height'] * matchups['partialPossessions']).sum() / matchups['partialPossessions'].sum()
                offender_weight = (matchups['Player Weight'] * matchups['partialPossessions']).sum() / matchups['partialPossessions'].sum()
                
                #ppm,ppp,team_ppm,team_ppp (weighted)
                def_ppm = (matchups['ppm'] * matchups['partialPossessions']).sum() / matchups['partialPossessions'].sum()
                def_ppp = (matchups['ppp'] * matchups['partialPossessions']).sum() / matchups['partialPossessions'].sum()
                def_team_ppm = (matchups['team_ppm'] * matchups['partialPossessions']).sum() / matchups['partialPossessions'].sum()
                def_team_ppp = (matchups['team_ppp'] * matchups['partialPossessions']).sum() / matchups['partialPossessions'].sum()
                
                
  
                
                
                defense_data_dict['Game_ID'].append(game_id)
                defense_data_dict['offender_size'].append(offender_size)
                defense_data_dict['offender_weight'].append(offender_weight)
                defense_data_dict['offender_height'].append(offender_height)
                defense_data_dict['Defppm'].append(def_ppm)
                defense_data_dict['Defppp'].append(def_ppp)
                defense_data_dict['Defteam_ppm'].append(def_team_ppm)
                defense_data_dict['Defteam_ppp'].append(def_team_ppp)

                
        
        
            offense_df = pd.DataFrame(offense_data_dict)
            player_gamelog = pd.merge(player_gamelog, offense_df, on='Game_ID', how='left')
            player_gamelog.to_csv(f'players/gamelogsv2/{player_name}_log.csv')
     
                                 
    #backcourt size relative to minutes 
    def get_todays_games(self):
        return scoreboard.ScoreBoard().games.get_dict()
    
    def read_players(self,backup=False):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        if backup != True:
            json_path = os.path.join(dir_path, 'players/player_json/player_info.json')
            with open(json_path) as json_file:
                return json.load(json_file)
        else:
            json_path = os.path.join(dir_path, 'players/player_json/backup_player_info.json')
            with open(json_path) as json_file:
                return json.load(json_file)

    def read_gameids(self):
        game_ids = []
        file_path = 'games/metadata/game_ids.txt'
        try:
            with open(file_path, 'r') as file:
                for line in file:
                    game_id = line.strip()
                    game_ids.append(game_id)
        except FileNotFoundError:
            print(f"Error: File '{file_path}' not found.")
        except Exception as e:
            print(f"An error occurred: {e}")

        return game_ids
        
    def get_gameids(self):
        game_ids = []
        for filename in os.listdir('players/gamelogs/'):
            file_path = f'players/gamelogs/{filename}'
            if '._' not in file_path:
                try:
                    player_df = pd.read_csv(file_path,delimiter=',')
                    # Use extend to add all Game_ID values to the game_ids list
                    game_ids.extend(player_df['Game_ID'])
                except KeyError:
                    try:
                        player_df = pd.read_csv(file_path,delimiter = '\t')
                        # Use extend to add all Game_ID values to the game_ids list
                        game_ids.extend(player_df['Game_ID'])
                    except KeyError:
                        pass
        
        #game_ids contains every unique Game_ID
        game_ids = list(set(game_ids))
        with open('games/metadata/game_ids.txt','w') as f:
            for item in game_ids:
                f.write(f'{item}\n')
                
        return game_ids
    
    
    
    
    
    def read_rosters(self):
        team_rosters = {}

        for player_name, player_info in self.players.items():
            player_id = player_info['id']
            team_abbreviation = player_info['TEAM_ABBREVIATION']
            team_id = player_info['TEAM_ID']
            position = player_info['POSITION']

            if team_abbreviation:
                player_data = {
                    'id': player_id,
                    'TEAMID': team_id,
                    'POSITION': position
                }

                # Add the player to the corresponding team in the team_rosters dictionary
                if team_abbreviation not in team_rosters:
                    team_rosters[team_abbreviation] = {}
                team_rosters[team_abbreviation][player_name] = player_data

        with open('teams/metadata/rosters/team_rosters.json', 'w') as file:
            json.dump(team_rosters, file)

        return team_rosters
    
    '''
    GAMELOGS (1)
    '''
    
    def build_player_dict(self):
        directory = 'games/2023-24/'
        player_dict = {}
        print('building player dict')
        for filepath in glob.glob(os.path.join(directory, '**/player_BoxScores.csv'), recursive=True):
            df = pd.read_csv(filepath)
            for _, row in df.iterrows():
                player_id = row['personId']
                full_name = f"{row['firstName']} {row['familyName']}"
                full_name = full_name.replace('.','')
                
                if full_name not in player_dict or player_dict[full_name]['id'] != player_id:
                    player_dict[full_name] = {"id": player_id}

        # Save the dictionary to a file
        with open('players/player_json/player_info.json', 'w') as file:
            json.dump(player_dict, file)
        
        return player_dict

    def player_meta(self, build_player_dict=True):
        if build_player_dict:  # only when updaing player_info json
            self.players = self.build_player_dict()
        else:
            self.players = self.read_players()

        retry_count = 3
        retry_delay = 5
        length = len(self.players)
        count = 0

        for player, attr in self.players.items():
            count += (1/length)
            print(f"{count*100:.2f} % -player meta")
            player_id = attr['id']

            for attempt in range(retry_count):
                try:
                    player_info = commonplayerinfo.CommonPlayerInfo(player_id)
                    break
                except ReadTimeout:
                    if attempt < retry_count - 1:
                        print(f"Timeout for {player}. Retrying {attempt + 1}/{retry_count} after {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        print(f"Failed to retrieve data for {player} after {retry_count} attempts.")
                        continue  # Skip to the next player after all retries

            meta = player_info.common_player_info.get_data_frame()
            selected_columns = ['HEIGHT', 'WEIGHT', 'BIRTHDATE', 'SEASON_EXP', 'TEAM_ID', 'TEAM_ABBREVIATION', 'POSITION', 'ROSTERSTATUS']
            for column in selected_columns:
                value = str(meta[column].iloc[0])
                try:
                    if column == 'HEIGHT':
                        feet, inches = value.split('-')
                        value = int(feet) * 30.48 + int(inches) * 2.54  # Convert to cm
                        value = round(value, 2)
                    if column == 'WEIGHT':
                        value = float(value)
                    if column == 'BIRTHDATE' and 'T' in str(value):
                        value = value.split('T')[0]
                except ValueError:
                    pass
                attr[column] = value


            time.sleep(1)  # Delay between processing each player

        with open('players/player_json/player_info.json', 'w') as file:
            json.dump(self.players, file)            
        with open('players/player_json/backup_player_info.json', 'w') as file:
            json.dump(self.players, file)
            

        print('Player meta completed (0)')
        return self.players

    
    def fetch_player_game_logs(self, player_id, season):
        try:
            game_logs = playergamelog.PlayerGameLog(player_id=player_id, season=season)
            logs_df = game_logs.get_data_frames()[0]
            return logs_df
        except Exception as e:
            return f"Error: {e}"
        
        
    def get_data(self, player_info_meta = False):
        if player_info_meta == True:
            self.players = self.player_meta()
        else:
            self.players = self.read_players()
        
        print('(1) getting gamelogs')
        percentage = 0
        for player, info in self.players.items():
            player_id = info['id']
            time.sleep(0.5)  # Be cautious with using time.sleep in production code
            result = self.fetch_player_game_logs(player_id, '2023-24')
            
            if isinstance(result, pd.DataFrame):
                self.create_cols(result)
                result.to_csv(f"players/gamelogs/{player}_log.csv", sep='\t', index=False)
                percentage += (1/len(self.players))
                print(round(percentage,4),' % - gamelog getter')
            else:
                print(f"Error fetching data for {player}: {result}")
        print('Finished player_game_log getter (1)')
        
    def create_cols(self,result):
        result[['Team', 'Location', 'Opponent']] = result['MATCHUP'].str.extract(r'([A-Z]+) ([@vs.]+) ([A-Z]+)')
        return result
    
    '''
    AVERAGES (2)
    '''
    
    def calculate_and_save_averages(self, file_path, game_type):
       
        current_data = pd.read_csv(file_path, delimiter='\t')
        selected_columns = ['MIN', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'REB', 'AST', 'STL', 'BLK', 'PTS', 'PLUS_MINUS']

        if game_type == 'home':
            filtered_data = current_data[current_data['MATCHUP'].str.contains('vs.')]
            output_folder = 'players/averages/home/'
        elif game_type == 'away':
            filtered_data = current_data[current_data['MATCHUP'].str.contains('@')]
            output_folder = 'players/averages/away/'
        else:
            filtered_data = current_data  # For combined, use all data
            output_folder = 'players/averages/combined/'

        column_averages = filtered_data[selected_columns].mean().round(2)
        averages_df = pd.DataFrame([column_averages], columns=selected_columns)
        averages_output_file_path = os.path.join(output_folder, os.path.basename(file_path))
        averages_df.to_csv(averages_output_file_path, index=False)

    def calc_save_averages(self):
        print('average getter')
        for filename in os.listdir('players/gamelogs/'):
            file_path = f'players/gamelogs/{filename}'
            if '._' not in file_path:
                try:
                    self.calculate_and_save_averages(file_path, 'home')
                    self.calculate_and_save_averages(file_path, 'away')
                    self.calculate_and_save_averages(file_path, 'combined')
                except TypeError:
                    pass
                except KeyError:
                    pass
        print('Player home, away, and combined averages calculation complete (2)')
        
    def append_averages_todict(self): #Appends player attributes to themselves
        players_to_remove = []
        
        
        #For predicting who guards who
        for player, attr in self.players.items():
            try:
                df = pd.read_csv(f'players/gamelogs/{player}_log.csv')
                # meta
                self.players[player]['AVG_MIN'] = round(df['MIN'].mean(), 2)
                self.players[player]['AVG_PACE'] = round(df['pace'].mean(), 2)
                self.players[player]['AVG_SPEED'] = round(df['speed'].mean(), 2)
                self.players[player]['AVG_DISTANCE'] = round(df['distance'].mean(), 2)
                self.players[player]['AVG_POSSESSIONS'] = round(df['possessions'].mean(), 2)
                self.players[player]['AVG_REB_PCT'] = round(df['reboundPercentage'].mean(), 2)
                self.players[player]['AVG_REB_CHANCES'] = round(df['reboundChancesTotal'].mean(), 2)
                self.players[player]['AVG_REB'] = round(df['REB'].mean(), 2)
                
                

                # poss
                self.players[player]['POSS_REB_CHANCES'] = round((df['reboundChancesTotal'] / df['possessions']).mean(), 2)

                # offense
                #offenisive possesions mean.
                
                self.players[player]['OFF_AVG_PTS'] = round(df['PTS'].mean(), 2)
                self.players[player]['OFF_AVG_AST'] = round(df['AST'].mean(), 2)
                self.players[player]['OFF_AVG_FGA'] = round(df['FGA'].mean(), 2)
                #self.players[player]['OFF_AVG_BLK'] = round(df['BLK'].mean(), 2)
                #self.players[player]['OFF_AVG_STL'] = round(df['STL'].mean(), 2)
                self.players[player]['OFF_AVG_3PA'] = round(df['FG3A'].mean(), 2)
                self.players[player]['OFF_AVG_OREB_PCT'] = round(df['offensiveReboundPercentage'].mean(), 2)
                self.players[player]['OFF_AVG_PTS_PAINT'] = round(df['pointsPaint'].mean(), 2)
                self.players[player]['OFF_AVG_OFF_RATING'] = round(df['offensiveRating'].mean(), 2)
                self.players[player]['OFF_AVG_USG_PCT'] = round(df['estimatedUsagePercentage'].mean(), 2)
                self.players[player]['OFF_AVG_TOUCHES'] = round(df['touches'].mean(), 2)
                self.players[player]['OFF_AVG_PASSES'] = round(df['passes'].mean(), 2)
                self.players[player]['OFF_AVG_TOV'] = round(df['TOV'].mean(), 2)
                

                # poss
                self.players[player]['OFF_POSS_PTS'] = round((df['PTS'] / df['possessions']).mean(), 2)
                self.players[player]['OFF_POSS_AST'] = round((df['AST'] / df['possessions']).mean(), 2)
                self.players[player]['OFF_POSS_REB'] = round((df['REB'] / df['possessions']).mean(), 2)
                self.players[player]['OFF_POSS_FGA'] = round((df['FGA'] / df['possessions']).mean(), 2)
                #self.players[player]['OFF_POSS_BLK'] = round((df['BLK'] / df['possessions']).mean(), 2)
                #self.players[player]['OFF_POSS_STL'] = round((df['STL'] / df['possessions']).mean(), 2)
                self.players[player]['OFF_POSS_PTS_PAINT'] = round((df['pointsPaint'] / df['possessions']).mean(), 2)
                self.players[player]['OFF_POSS_PTS_OFFTOV'] = round((df['pointsOffTurnovers'] / df['possessions']).mean(), 2)
                self.players[player]['OFF_POSS_OREB_CHANCES'] = round((df['reboundChancesOffensive'] / df['possessions']).mean(), 2)
                self.players[player]['OFF_POSS_SCREEN_AST'] = round((df['screenAssists'] / df['possessions']).mean(), 2)
                self.players[player]['OFF_POSS_PTS_SECONDCHANCE'] = round((df['pointsSecondChance'] / df['possessions']).mean(), 2)
                self.players[player]['OFF_POSS_PTS_OFFTOV'] = round((df['pointsOffTurnovers'] / df['possessions']).mean(), 2)
                self.players[player]['OFF_POSS_PIE'] = round((df['PIE'] / df['possessions']).mean(), 2)
                self.players[player]['OFF_POSS_PASSES'] = round((df['passes'] / df['possessions']).mean(), 2)
                self.players[player]['OFF_POSS_TOUCHES'] = round((df['touches'] / df['possessions']).mean(), 2)
                self.players[player]['OFF_POSS_3PA'] = round((df['FG3A'] / df['possessions']).mean(), 2)
                self.players[player]['OFF_POSS_USG_PCT'] = round((df['estimatedUsagePercentage'] / df['possessions']).mean(), 2)
                
                
                
                
                
                #defense
                self.players[player]['DEF_AVG_PTS'] = round(df['oppPoints'].mean(), 2)
                self.players[player]['DEF_AVG_AST'] = round(df['matchupAssists'].mean(), 2)
                self.players[player]['DEF_AVG_FGA'] = round(df['matchupFieldGoalsAttempted'].mean(), 2)
                #self.players[player]['DEF_AVG_BLK'] = round(df['BLK'].mean(), 2)
                #self.players[player]['DEF_AVG_STL'] = round(df['STL'].mean(), 2)
                self.players[player]['DEF_AVG_3PA'] = round(df['matchupThreePointersAttempted'].mean(), 2)
                self.players[player]['DEF_AVG_DREB_PCT'] = round(df['defensiveReboundPercentage'].mean(), 2)
                #self.players[player]['DEF_AVG_PTS_PAINT'] = round(df['pointsPaint'].mean(), 2)
                self.players[player]['DEF_AVG_DEF_RATING'] = round(df['defensiveRating'].mean(), 2)
                #self.players[player]['DEF_AVG_USG_PCT'] = round(df['estimatedUsagePercentage'].mean(), 2)
                self.players[player]['DEF_AVG_TOUCHES'] = round(df['touches'].mean(), 2)
                self.players[player]['DEF_AVG_PASSES'] = round(df['passes'].mean(), 2)
                self.players[player]['DEF_TOV'] = round(df['matchupTurnovers'].mean(), 2)
                
                
                
                #poss
                
                self.players[player]['DEF_POSS_PTS'] = round((df['oppPoints'] / df['possessions']).mean(), 2)
                self.players[player]['DEF_POSS_AST'] = round((df['matchupAssists'] / df['possessions']).mean(), 2)
                self.players[player]['DEF_POSS_REB'] = round((df['REB'] / df['possessions']).mean(), 2)
                self.players[player]['DEF_POSS_FGA'] = round((df['matchupFieldGoalsAttempted'] / df['possessions']).mean(), 2)
                #self.players[player]['DEF_POSS_BLK'] = round((df['BLK'] / df['possessions']).mean(), 2)
                #self.players[player]['DEF_POSS_STL'] = round((df['STL'] / df['possessions']).mean(), 2)
                self.players[player]['DEF_POSS_PTS_PAINT'] = round((df['pointsPaint'] / df['possessions']).mean(), 2)
                self.players[player]['DEF_POSS_PTS_OFFTOV'] = round((df['pointsOffTurnovers'] / df['possessions']).mean(), 2)
                self.players[player]['DEF_POSS_OREB_CHANCES'] = round((df['reboundChancesOffensive'] / df['possessions']).mean(), 2)
                self.players[player]['DEF_POSS_SCREEN_AST'] = round((df['screenAssists'] / df['possessions']).mean(), 2)
                self.players[player]['DEF_POSS_PTS_SECONDCHANCE'] = round((df['pointsSecondChance'] / df['possessions']).mean(), 2)
                self.players[player]['DEF_POSS_PTS_OFFTOV'] = round((df['pointsOffTurnovers'] / df['possessions']).mean(), 2)
                self.players[player]['DEF_POSS_PIE'] = round((df['PIE'] / df['possessions']).mean(), 2)
                self.players[player]['DEF_POSS_PASSES'] = round((df['passes'] / df['possessions']).mean(), 2)
                self.players[player]['DEF_POSS_TOUCHES'] = round((df['touches'] / df['possessions']).mean(), 2)
                self.players[player]['DEF_POSS_3PA'] = round((df['matchupThreePointersAttempted'] / df['possessions']).mean(), 2)
                self.players[player]['DEF_POSS_USG_PCT'] = round((df['estimatedUsagePercentage'] / df['possessions']).mean(), 2)
                self.players[player]['DEF_TOV'] =  round((df['matchupTurnovers'] / df['possessions']).mean(), 2)
                self.players[player]['DEF_AVG_DREB_PCT'] =  round((df['defensiveReboundPercentage'] / df['possessions']).mean(), 2)
                

                    
            except FileNotFoundError:
                print(player,'File N/A Error')
        
            except KeyError:
                print(player,'Key Error..Removed')
                players_to_remove.append(player)
        
        
        
                
        for player, stats in self.players.items():
            inf_values = {key: value if np.isreal(value) and not np.isinf(value) else 0 for key, value in stats.items()}
            self.players[player] = inf_values
                
        for player_to_remove in players_to_remove:
            del self.players[player_to_remove]
        
                
        with open('players/player_json/player_info.json', 'w') as file:
            json.dump(self.players, file)
            
            #again, ir pd read csv, player meta
            #which has the players custom ratings. Ie player total score, offense/defense score, matchup vs bigger/smaller and lighter/heavier opponents, score
        
        
    '''
    DEFENSE VS POSITION
    '''
    
    def fetch_and_save_dvpos(self, timeframe):
        filename = f"players/defense_vpos/team_defense_vpos_{timeframe}.json"
        params = {'range': timeframe}
        dvpos_headers = {
            'x-api-key': 'CHi8Hy5CEE4khd46XNYL23dCFX96oUdw6qOt1Dnh'  # Be cautious with API keys in code
        }
        response = requests.get(self.api_base_url, headers=dvpos_headers, params=params)

        if response.status_code == 200:
            with open(filename, 'w') as file:
                json.dump(response.json(), file, indent=4)
            #print(f"Data for timeframe {timeframe} saved to {filename}.")
        else:
            print(f"Failed to fetch data for timeframe {timeframe}. Status code: {response.status_code}")

    def acquire_dvpos(self):
        for timeframe in self.timeframes:
            self.fetch_and_save_dvpos(timeframe)
        print("team defense v pos complete (3)")
    
    '''
    MEDIANS (4)
    '''

    def calculate_and_save_medians(self, file_path, game_type):
        current_data = pd.read_csv(file_path, delimiter='\t')
        selected_columns = ['MIN', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 'REB', 'AST', 'STL', 'BLK', 'PTS', 'PLUS_MINUS']
        try:
            if game_type == 'home':
                filtered_data = current_data[current_data['MATCHUP'].str.contains('vs.')]
                output_folder = 'players/medians/home/'
            elif game_type == 'away':
                filtered_data = current_data[current_data['MATCHUP'].str.contains('@')]
                output_folder = 'players/medians/away/'
            else:
                filtered_data = current_data  # For combined, use all data
                output_folder = 'players/medians/combined/'

            column_medians = filtered_data[selected_columns].median().round(2)
            medians_df = pd.DataFrame([column_medians], columns=selected_columns)
            medians_output_file_path = os.path.join(output_folder, os.path.basename(file_path))
            medians_df.to_csv(medians_output_file_path, index=False)
        except KeyError:
            pass

    def acquire_medians(self):
        for filename in os.listdir('players/gamelogs/'):
            file_path = f'players/gamelogs/{filename}'
            if '._' not in file_path:
                try:
                    self.calculate_and_save_medians(file_path, 'home')
                    self.calculate_and_save_medians(file_path, 'away')
                    self.calculate_and_save_medians(file_path, 'combined')
                except TypeError:
                    pass
        print('Player home, away, and combined medians calculation complete (4)')

    '''
    PROPS (5)
    '''

    def fetch_game_ids_for_props(self):
        api_date = time.strftime("%Y-%m-%d")
        
        game_endpoint = f"https://api.prop-odds.com/beta/games/nba?date={api_date}&tz=America/New_York&api_key={self.prop_api_key}"
        response = requests.get(game_endpoint)
        if response.status_code == 200:
            data = response.json()
            game_ids = [game["game_id"] for game in data["games"]]
            with open('props/game_ids.txt', 'w') as file:
                json.dump(game_ids, file)  # Provide the file pointer as the first argument
            return game_ids
        else:
            print(f"Failed to fetch game IDs. Status code: {response.status_code}")
            return []


    def fetch_prop_data_for_game(self, game_id):
        for market in self.prop_markets:
            market_endpoint = f'https://api.prop-odds.com/beta/odds/{game_id}/{market}?api_key={self.prop_api_key}'
            resp = requests.get(market_endpoint)
            if resp.status_code == 200:
                market_data = resp.json()['sportsbooks'][2]['market']
                self.prop_data.append(market_data)

    def save_data_to_file(self):
        with open(self.props_filename, 'w') as file:
            json.dump(self.prop_data, file, indent=4)

    def acquire_props(self, force_update=False):
        if not os.path.exists(self.props_filename) or force_update:
            game_ids = self.fetch_game_ids_for_props()
            for game_id in game_ids:
                self.fetch_prop_data_for_game(game_id)
            if self.prop_data:
                self.save_data_to_file()
                print(f"Prop data saved to {self.props_filename}")
            print('Props acquisition complete (5)')
        else:
            print(f"Odds file already exists: {self.props_filename}")
        
    '''
    STANDARD DEVIATION (6)
    '''

    def iterate_through_std(self):
        for filename in os.listdir('players/gamelogs/'):
            file_path = f'players/gamelogs/{filename}'
            if '._' not in file_path:
                self.calculate_and_save_std(file_path)
        print('Standard Deviation Complete (6)')
                    
                    
    def calculate_and_save_std(self, file_path):
        try:
            current_data = pd.read_csv(file_path, delimiter='\t')
            std_devs = current_data[['PTS', 'REB', 'AST']].std()

            # Create a DataFrame correctly
            std_df = pd.DataFrame([std_devs.values], columns=['PTS', 'REB', 'AST'])
            save_path = os.path.join('players/standard_deviations', os.path.basename(file_path))
            std_df.to_csv(save_path, index=False)
            
        except Exception as e:
            print(f"An error occurred while processing {file_path}: {e}")

    '''
    TEAM METRICS (7)
    '''
    
    def get_team_standings(self):
        standings = leaguestandings.LeagueStandings().get_data_frames()[0]
        standings['TEAM_NAME'] = standings['TeamCity'] + ' ' + standings['TeamName']
        standings = standings.loc[:, ['TEAM_NAME','Conference','Division','WinPCT','HOME','ROAD','PointsPG','OppPointsPG','DiffPointsPG']]
        cols_to_zscore = ['WinPCT','PointsPG','OppPointsPG','DiffPointsPG']
        standings[cols_to_zscore] = zscore(standings[cols_to_zscore])
        
        return standings

    def get_team_metrics(self):
        team_metrics = teamestimatedmetrics.TeamEstimatedMetrics().get_data_frames()[0]
        team_metrics = team_metrics.loc[:, ['TEAM_NAME','W_PCT','E_OFF_RATING','E_DEF_RATING','E_NET_RATING','E_PACE','E_REB_PCT','E_TM_TOV_PCT']]    
        cols_to_zscore = ['W_PCT','E_OFF_RATING','E_DEF_RATING','E_NET_RATING','E_PACE','E_REB_PCT','E_TM_TOV_PCT']
        team_metrics[cols_to_zscore] = zscore(team_metrics[cols_to_zscore])
        team_metrics['E_DEF_RATING'] = -1 * team_metrics['E_DEF_RATING']      
        
        return team_metrics


    def combine_team_dfs(self):
        team_standings = self.get_team_standings()
        team_metrics = self.get_team_metrics()  
        team_data = pd.merge(team_standings, team_metrics, on = 'TEAM_NAME')
        self.abbreviation(team_data)
        team_data.to_csv('teams/metadata/team_data_zscore.csv', index=False)
        print('Team Metrics complete (7)')
    
    def abbreviation(self, df):
        key = {
            'ATL': 'Atlanta Hawks', 'BOS': 'Boston Celtics', 'BKN': 'Brooklyn Nets', 'CHA': 'Charlotte Hornets',
            'CHI': 'Chicago Bulls', 'CLE': 'Cleveland Cavaliers', 'DAL': 'Dallas Mavericks', 'DEN': 'Denver Nuggets',
            'DET': 'Detroit Pistons', 'GSW': 'Golden State Warriors', 'HOU': 'Houston Rockets', 'IND': 'Indiana Pacers',
            'LAC': 'LA Clippers', 'LAL': 'Los Angeles Lakers', 'MEM': 'Memphis Grizzlies', 'MIA': 'Miami Heat',
            'MIL': 'Milwaukee Bucks', 'MIN': 'Minnesota Timberwolves', 'NOP': 'New Orleans Pelicans', 'NYK': 'New York Knicks',
            'OKC': 'Oklahoma City Thunder', 'ORL': 'Orlando Magic', 'PHI': 'Philadelphia 76ers', 'PHX': 'Phoenix Suns',
            'POR': 'Portland Trail Blazers', 'SAC': 'Sacramento Kings', 'SAS': 'San Antonio Spurs', 'TOR': 'Toronto Raptors',
            'UTA': 'Utah Jazz', 'WAS': 'Washington Wizards'
        }
        df['Team'] = df['TEAM_NAME'].map({v: k for k, v in key.items()})
        return df
    
    '''
    PLAYER AND TEAM METRIC MERGER (8)
    '''
    def merge_teamplayer_data(self):
        team_metrics = pd.read_csv('teams/metadata/team_data_zscore.csv')
        for filename in os.listdir('players/gamelogs/'):
            file_path = os.path.join('players/gamelogs/', filename)
            if '._' or '_.' not in filepath:
                try:
                    player_log = pd.read_csv(file_path,delimiter='\t')
                    for index, row in player_log.iterrows():
                        team_data = team_metrics[team_metrics['Team'] == row['Opponent']] 
                        for col in ['W_PCT','E_OFF_RATING','E_DEF_RATING','E_NET_RATING','E_PACE','E_REB_PCT']:
                            if not team_data.empty:
                                player_log.at[index, col] = team_data.iloc[0][col]
            
                    player_log.to_csv(file_path, index=False)
                    
                except UnicodeDecodeError:
                    pass
        print('Player and Team Metric Merger complete (8)')


    
    
    '''
    NBA API (9)
    '''
    
    def clean_df(self,df,t):
        columns_to_drop = [col for col in df.columns if '_x' in col or '_y' in col]
        dataframe = df.drop(columns=columns_to_drop)
        dataframe.rename(columns={'playerPoints': 'oppPoints'}, inplace=True)
        
        if t == 'player_df':
            for index,row in dataframe.iterrows():
                try:
                    player_name = (dataframe.loc[index,'firstName'] + ' ' + dataframe.loc[index,'familyName'])
                    player_name = player_name.replace('.','')
                    dataframe.loc[index,'Player Name'] = player_name
                    dataframe.loc[index, 'Player Height'] = self.players[player_name]['HEIGHT']
                    dataframe.loc[index, 'Player Weight'] = self.players[player_name]['WEIGHT']
                    dataframe.loc[index, 'True Position'] = self.players[player_name]['POSITION']
                except KeyError:
                    print('KeyError')
            
        return dataframe
    
    
    #def calcGameMeta(self,df):
        
        
        
        
        
    def accessNBA_API_boxscores(self):
        file_path = 'games/2023-24'
        retry_count = 3
        retry_delay = 3  # Starting delay
        count = 0
        x = 0

        for gameid in self.game_ids:
            gameid = f'00{gameid}'
            if os.path.exists(f'{file_path}/{gameid}'):
                print('Access NBAAPI boxscores Game exists: ',gameid, 'Count: ',count)
                count += (1/len(self.game_ids))
                pass
            else:
                for attempt in range(retry_count):
                    try:
                        count += (1/len(self.game_ids))
                        time.sleep(0.5)  # Increasing the delay
                        
                        HUSTLE = boxscorehustlev2.BoxScoreHustleV2(game_id = gameid)
                        DEFENSIVE = boxscoredefensivev2.BoxScoreDefensiveV2(game_id = gameid)
                        ADVANCED = boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=gameid)
                        MISC = boxscoremiscv3.BoxScoreMiscV3(game_id=gameid)
                        TRACK = boxscoreplayertrackv3.BoxScorePlayerTrackV3(game_id=gameid)
                        USAGE = boxscoreusagev3.BoxScoreUsageV3(game_id=gameid)
                        SCORING = boxscorescoringv3.BoxScoreScoringV3(game_id=gameid)
                
                        player_hustle_stats = HUSTLE.player_stats.get_data_frame()
                        player_defensive_stats = DEFENSIVE.player_stats.get_data_frame()
                        player_advanced_stats = ADVANCED.player_stats.get_data_frame()
                        player_misc_stats = MISC.player_stats.get_data_frame()
                        player_track_stats = TRACK.player_stats.get_data_frame()
                        player_usage_stats = USAGE.player_stats.get_data_frame()
                        player_scoring_stats = SCORING.player_stats.get_data_frame()
                        team_hustle_stats = HUSTLE.team_stats.get_data_frame()
                        team_advanced_stats = ADVANCED.team_stats.get_data_frame()
                        team_misc_stats = MISC.team_stats.get_data_frame()
                        team_track_stats = TRACK.team_stats.get_data_frame()
                        team_usage_stats = USAGE.team_stats.get_data_frame()
                        team_scoring_stats = SCORING.team_stats.get_data_frame()
                        
                        
                        player_df1 = pd.merge(player_hustle_stats,player_defensive_stats, on = 'personId',how = 'outer')
                        player_df2 = pd.merge(player_advanced_stats,player_misc_stats, on = 'personId',how = 'outer')
                        player_df3 = pd.merge(player_track_stats,player_usage_stats, on = 'personId',how = 'outer')
                        player_df = pd.merge(player_df1,player_scoring_stats, on = 'personId',how = 'outer')
                        player_df = pd.merge(player_df,player_df2, on = 'personId',how = 'outer')
                        player_df = pd.merge(player_df,player_df3, on = 'personId',how = 'outer')
                        team_df1 = pd.merge(team_hustle_stats,team_advanced_stats, on = 'teamId',how='outer')
                        team_df2 = pd.merge(team_misc_stats,team_track_stats, on = 'teamId',how='outer')
                        team_df3 = pd.merge(team_usage_stats, team_scoring_stats, on = 'teamId',how='outer')
                        team_df = pd.merge(team_df1, team_df2, on = 'teamId',how='outer')
                        team_df = pd.merge(team_df, team_df3, on = 'teamId',how='outer')
                           
                        player_df = self.clean_df(player_df,t='player_df')
                        team_df = self.clean_df(team_df,t='team_df')
                        
                        
                        
                        os.makedirs(os.path.join(file_path, gameid), exist_ok=True)
                        with open(f'{file_path}/{gameid}/player_BoxScores.csv', 'w') as playerdf_file:
                            player_df.to_csv(playerdf_file, index=False)
                            
                        with open(f'{file_path}/{gameid}/team_BoxScores.csv', 'w') as teamdf_file:
                            team_df.to_csv(teamdf_file, index=False)
                          
                        print('Count: ',count,'Success!')
                        break
                    
                    except ReadTimeout:
                        print(f"Timeout for game ID {gameid}, attempt {attempt + 1}/{retry_count}. Retrying after {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay += 2  # Exponentially increase the delay
                    except AttributeError:
                        print('Game_ID failed: ',gameid)
                    except JSONDecodeError:
                        pass
                    
                    
                

        print('Game boxscores saved (9)')

    def add_boxscoregamelog(self):
        print('Concatenating player gamelogs and boxscores')
        for filename in os.listdir('players/gamelogs/'):
            file_path = os.path.join('players/gamelogs/', filename)
            if '._' or '_.' not in filepath:
                try:
                    all_rows = []
                    player_log = pd.read_csv(file_path)
                    for index, row in player_log.iterrows():
                        gameid = row['Game_ID']
                        gameid = f'00{gameid}'
                        playerid = row['Player_ID']
                        
                        game_path = f'games/2023-24/{gameid}/player_BoxScores.csv'
                        if os.path.exists(game_path):
                            game = pd.read_csv(game_path)
                            matching_game_row = game[game['personId'] == playerid]

                            if not matching_game_row.empty:
                                merged_row = pd.merge(row.to_frame().T, matching_game_row, left_on='Player_ID', right_on='personId', how='left')
                                all_rows.append(merged_row)
                            else:
                                print(f"No matching row for Player_ID {playerid} in game {gameid}")
                        else:
                            #print(f"Game file not found: {game_path}")
                            pass

                    if all_rows:
                        final_df = pd.concat(all_rows, ignore_index=True)
                        final_df.to_csv(file_path,index=False)
                    else:
                        print(f"No data to concatenate for file {filename}")

                except TypeError as e:
                    print(f"KeyError processing file {filename}: {e}")
                except UnicodeDecodeError:
                    pass
        print('Gamelog and box scores concatenated! (10)')
        
    def read_teams(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        json_path = os.path.join(dir_path, 'teams/metadata/NBA_TeamIds.json')
        with open(json_path) as json_file:
            return json.load(json_file)
        
    def get_teamStats(self):
        directory = 'teams/games/gamelogs'
        if not os.path.exists(directory):
            os.makedirs(directory)
            
        league_gamelog = leaguegamelog.LeagueGameLog(season=self.season).league_game_log.get_data_frame()

        time.sleep(0.5)
        for team, attr in self.teams.items():
            teamid = attr['id']
            team_abbreviation = team
            time.sleep(0.5)
            team_gamelog = teamgamelog.TeamGameLog(season=self.season, team_id=teamid).get_data_frames()[0]
            league_gamelog_filtered = league_gamelog[league_gamelog['TEAM_ABBREVIATION'] != team_abbreviation]
            merged_gamelog = team_gamelog.merge(league_gamelog_filtered, left_on='Game_ID', right_on='GAME_ID', suffixes=('', '_opponent'))

            merged_gamelog['Point_Diff'] = merged_gamelog['PTS'].astype(int) - merged_gamelog['PTS_opponent'].astype(int)
            merged_gamelog['Blowout'] = merged_gamelog['Point_Diff'].apply(lambda x: abs(x) >= 15)
            
            blowout_games = merged_gamelog['Blowout'].sum()
            total_games = len(merged_gamelog)
            blowout_rate = round(((blowout_games / total_games)*100),2)
            

            
            filename = f"{directory}/{team}.csv"
            merged_gamelog.to_csv(filename, index=False)
           

    def add_cols(self,df):
        df['TCHS_MIN'] = (df['touches'] / df['MIN'])
        df['PASSES_MIN'] = (df['passes'] / df['MIN'])
        df['REB_CHANCES_MIN'] = (df['reboundChancesTotal'] / df['MIN'])
        df['POSS_MIN'] = (df['possessions'] / df['MIN'])
        df['PTSPAINT_MIN'] = (df['pointsPaint'] / df['MIN'])
        
        df['opp_PTSPAINT_MIN'] = (df['oppPointsPaint'] / df['MIN'])
        
        return df
    def add_team_boxscoregamelog(self):
        print('Concatenating team gamelogs and boxscores')
        for filename in os.listdir('teams/games/gamelogs/'):
            file_path = os.path.join('teams/games/gamelogs/', filename)
            if '._' or '_.' not in filepath:
                try:
                    all_rows = []
                    team_log = pd.read_csv(file_path)
                    for index, row in team_log.iterrows():
                        gameid = row['Game_ID']
                        gameid = f'00{gameid}'
                        teamid = row['Team_ID']
                        
                        game_path = f'games/2023-24/{gameid}/team_BoxScores.csv'
                        if os.path.exists(game_path):
                            game = pd.read_csv(game_path)
                            matching_game_row = game[game['teamId'] == teamid]

                            if not matching_game_row.empty:
                                merged_row = pd.merge(row.to_frame().T, matching_game_row, left_on='Team_ID', right_on='teamId', how='left')
                                all_rows.append(merged_row)
                            else:
                                print(f"No matching row for Team_ID {teamid} in game {gameid}")
                        else:
                            #print(f"Game file not found: {game_path}")
                            pass

                    if all_rows:
                        final_df = pd.concat(all_rows, ignore_index=True)
                        final_df = self.add_cols(final_df)
                        
                        final_df.to_csv(file_path,index=False)
                    else:
                        print(f"No data to concatenate for file {filename}")

                except TypeError as e:
                    print(f"KeyError processing file {filename}: {e}")
                except UnicodeDecodeError:
                    pass
        print('Gamelog and box scores concatenated! (11)')
    
    def get_injuries(self):
        site_url = 'https://www.rotowire.com/basketball/tables/injury-report.php'
        params = {'team': 'ALL', 'pos': 'ALL'}
        site_response = requests.get(site_url, params=params)
        inj_json = site_response.json()

        for player in inj_json:
            original_player_name = player['player']
            team, injury, status = player['team'], player['injury'], player['status']

            clean_player_name = original_player_name.replace('.', '')
            if team in self.rosters and clean_player_name in self.rosters[team]:
                if 'Out' in status:
                    self.rosters[team].pop(clean_player_name)
            else:
                player_name_with_jr = original_player_name + ' Jr'
                if team in self.rosters and player_name_with_jr in self.rosters[team]:
                    if 'Out' in status:
                        self.rosters[team].pop(player_name_with_jr)

    def determine_opp_method(self, partial_possessions):
        if partial_possessions > 25:
            return '1'
        elif 10 <= partial_possessions <= 24:
            return '2'
        else:
            return '3'
        
    
    def saveMatchups(self, mode='offense'):
        print('selfMatchups Starting')
        all_matchups_df = pd.DataFrame()
        count = 0
        retry_count = 3
        retry_delay = 3
       
       
        for game_id in self.game_ids:
            game_id = f'00{game_id}'
            success = False
            count += 1
            print(('saveMatchups',count/len(self.game_ids)))
            for attempt in range(retry_count):
                try:
                    time.sleep(0.5)
                    game = boxscorematchupsv3.BoxScoreMatchupsV3(game_id=game_id)
                    game_player_stats = game.player_stats.get_data_frame()
                    player_stats_df = self.save_and_print_player_metrics(game_player_stats)
                    all_matchups_df = pd.concat([all_matchups_df, player_stats_df], ignore_index=True)
                    success = True
                   
                    break
                except ReadTimeout:
                    print(f"Timeout for game ID {game_id}, attempt {attempt + 1}/{retry_count}. Retrying after {retry_delay} seconds...")
                    time.sleep(retry_delay)
                except IndexError as e:
                    print(f'error {e}')

            if not success:
                print(f"Failed to retrieve data for game ID {game_id} after {retry_count} attempts.")
                


        try:
            for index, row in all_matchups_df.iterrows():
                player_name = row['Player Name']
                opponent_name = row['Opponent']
                player_height, player_position, player_weight = self.players[player_name]['HEIGHT'], self.players[player_name]['POSITION'], self.players[player_name]['WEIGHT']
                opponent_height, opponent_position, opponent_weight, opponent_minutes = self.players[opponent_name]['HEIGHT'], self.players[opponent_name]['POSITION'], self.players[opponent_name]['WEIGHT'], self.players[opponent_name]['AVG_MIN']

                #add opponent metrics here from self.players[player][**stat**]
        
                player_team = self.players[player_name]['TEAM_ABBREVIATION']
                opponent_team = self.players[opponent_name]['TEAM_ABBREVIATION']
                
                #offensive player                
                all_matchups_df.at[index, 'Player Position'] = player_position
                all_matchups_df.at[index, 'Player Team'] = player_team
                all_matchups_df.at[index, 'Player Height'] = player_height
                all_matchups_df.at[index, 'Player Weight'] = player_weight
                all_matchups_df.at[index, 'Average Passes per Poss'] = self.players[player_name]['OFF_POSS_PASSES']
                
                

                #defensive player
                all_matchups_df.at[index, 'Opponent Position'] = opponent_position
                all_matchups_df.at[index, 'Opponent Height'] = opponent_height
                all_matchups_df.at[index, 'Opponent Weight'] = opponent_weight
                all_matchups_df.at[index, 'Avg Min'] = opponent_minutes

        except KeyError as e:
            print(f"Key error: {e}")
            
        #all_matchups_df['Player Height'] = self.players[all_matchups_df['Player Name']]['Player Height']
        all_matchups_df['Matchup Minutes'] = all_matchups_df['Matchup Minutes'].apply(lambda x: round(int(x.split(':')[0]) + int(x.split(':')[1]) / 60, 2) if isinstance(x, str) else x)
        all_matchups_df['Player Height'] = pd.to_numeric(all_matchups_df['Player Height'], errors='coerce')
        all_matchups_df['Player Weight'] = pd.to_numeric(all_matchups_df['Player Weight'], errors='coerce')
        all_matchups_df['Opponent Height'] = pd.to_numeric(all_matchups_df['Opponent Height'], errors='coerce')
        all_matchups_df['Opponent Weight'] = pd.to_numeric(all_matchups_df['Opponent Weight'], errors='coerce')
        all_matchups_df['ppm'] = round(all_matchups_df['Player Points'] / all_matchups_df['Matchup Minutes'], 2)
        all_matchups_df['ppp'] = round(all_matchups_df['Player Points'] / all_matchups_df['partialPossessions'], 2)
        all_matchups_df['team_ppm'] = round(all_matchups_df['Team Points'] / all_matchups_df['Matchup Minutes'], 2)
        all_matchups_df['team_ppp'] = round(all_matchups_df['Team Points'] / all_matchups_df['partialPossessions'], 2)
        all_matchups_df['POSS_FGA'] = round(all_matchups_df['partialPossessions'] / all_matchups_df['matchupFieldGoalsAttempted'], 2)


        

        player_grouped = all_matchups_df.groupby('Player Name')
        all_matchups_df = all_matchups_df.replace(np.inf, np.nan)
        all_matchups_df.to_csv('players/matchups/data/orig_matchup.csv', index=False)

        player_metrics = {}
        for player in self.players:
            # Filter data for the player
            player_df = all_matchups_df[all_matchups_df['Player Name'] == player]
            opponent_df = all_matchups_df[all_matchups_df['Opponent'] == player]

            total_matchup_mins = player_df['Matchup Minutes'].sum()
            total_possessions = player_df['partialPossessions'].sum()
            total_player_points = player_df['Player Points'].sum()

            total_matchup_mins_opp = opponent_df['Matchup Minutes'].sum()
            total_possessions_opp = opponent_df['partialPossessions'].sum()
            total_player_points_opp = opponent_df['Player Points'].sum()

            offRtg = total_player_points / total_possessions if total_possessions != 0 else 0
            defRtg = total_player_points_opp / total_possessions_opp if total_possessions_opp != 0 else 0

            # average size and weight of people guarded
            height_guarded = sum(
                (opponent_df['partialPossessions'] / total_possessions_opp) * opponent_df['Player Height'])
            weight_guarded = sum(
                (opponent_df['partialPossessions'] / total_possessions_opp) * opponent_df['Player Weight'])

            try:
                player_id = self.players[player]['id']
                player_height = round(float(self.players[player]['HEIGHT']),2)
                player_weight = round(float(self.players[player]['WEIGHT']),2)
                position = self.players[player]['POSITION']
                #add points/game and ast per game
                Avg_Min = self.players[player]['AVG_MIN']
                Avg_Pts = self.players[player]['OFF_AVG_PTS']
                Avg_Ast = self.players[player]['OFF_AVG_AST']

                team = self.players[player]['TEAM_ABBREVIATION']

                player_metrics[player] = {
                    'Player ID': player_id,
                    'Player Team': team,
                    'Position': position,
                    'Avg Min': Avg_Min,
                    'Avg Pts':Avg_Pts,
                    'Avg Ast':Avg_Ast,
                    'Offensive Rating': round(offRtg, 2),
                    'Defensive Rating': round(defRtg, 2),
                    'Player Height': player_height,
                    'Player Weight': player_weight,
                }
                

                if mode == 'defense':
                    for index, row in opponent_df.iterrows():
                        opponent_name = row['Player Name']
                        # Use opponent's 'AVG_MIN' for defensive dataframe
                        #defense should be average stat/game as well as average stat/min, 
                        opponent_df.loc[index, 'Avg Min'] = self.players[opponent_name]['AVG_MIN']
                        opponent_df.loc[index, 'Avg Pts'] = self.players[opponent_name]['OFF_AVG_PTS']
                        opponent_df.loc[index, 'Avg Ast'] = self.players[opponent_name]['OFF_AVG_AST']
                        #opponent_df.loc[index, 'Off Talent'] = self.players[opponent_name]['
                elif mode == 'offense':
                    for index,row in player_df.iterrows():
                        opponent_name = row['Opponent']
                        player_df.loc[index, 'Defender Avg Min'] = self.players[opponent_name]['AVG_MIN']
                        player_df.loc[index, 'Defender Avg Pts Against'] = self.players[opponent_name]['DEF_AVG_PTS']
                        player_df.loc[index, 'Defender Poss Pts Against'] = self.players[opponent_name]['DEF_POSS_PTS']
                        player_df.loc[index, 'Defender Avg Ast Against'] = self.players[opponent_name]['DEF_AVG_AST']

            except ValueError:
                pass
            '''
            except KeyError:
                print(player, 'Error')
            '''

            if mode == 'offense':
                player_df.to_csv(f'players/matchups/data/{mode}/{player}_matchups.csv', index=False)
            else:
                opponent_df.to_csv(f'players/matchups/data/{mode}/{player}_matchups.csv', index=False)

        # Convert the dictionary to a DataFrame
        player_metrics_df = pd.DataFrame.from_dict(player_metrics, orient='index')
        player_metrics_df = player_metrics_df.dropna()

        # Save the DataFrame to a CSV file
        player_metrics_df.to_csv('players/matchups/metadata/player_matchups.csv', index_label='Player Name')
        
        
        
    def determine_opp_method(self,partial_possessions):
        if partial_possessions > 25:
            return '1'
        elif 10 <= partial_possessions <= 24:
            return '2'
        else:
            return '3'
        
    def save_and_print_player_metrics(self, game_data):
        if not isinstance(game_data, pd.DataFrame):
            raise ValueError("game_data must be a pandas DataFrame")

        columns = ['Game_Id','Player Name', 'Player Position','playerId', 'Opponent', 'Opponent Position','opponentId', 'Matchup Minutes', "partialPossessions", 
                   'Player Points','ppm','ppp', 'Team Points','team_ppm','team_ppp', 'Matchup Assists', 
                   'matchupThreePointersAttempted', 'matchupThreePointersMade', 'matchupFreeThrowsAttempted', 
                   'matchupFieldGoalsMade', 'matchupFieldGoalsAttempted', 'matchupFieldGoalsPercentage', 
          "matchupFreeThrowsMade","shootingFouls",'Player Height','Player Weight','Opponent Height','Opponent Weight']
        new_rows = []

        for index, record in game_data.iterrows():
            #print(record)
            player_name = f"{record['firstNameOff']} {record['familyNameOff']}"
            player_name = player_name.replace('.','')
            opponent = f"{record['firstNameDef']} {record['familyNameDef']}"
            opponent = opponent.replace('.', '')
            #add team and opponent team names
            new_row = {
                'Game_Id': record['gameId'],
                'Player Name': player_name,
                'playerId': record['personIdOff'],
                'Opponent': opponent,
                'opponentId': record['personIdDef'],
                'Matchup Minutes': record['matchupMinutes'],
                'partialPossessions': record["partialPossessions"],
                'Player Points': record['playerPoints'],
                'Team Points': record['teamPoints'],
                'Matchup Assists': record['matchupAssists'],
                'matchupThreePointersAttempted': record['matchupThreePointersAttempted'],
                'matchupThreePointersMade': record['matchupThreePointersMade'],
                'matchupFreeThrowsAttempted': record['matchupFreeThrowsAttempted'],
                'matchupFieldGoalsMade': record['matchupFieldGoalsMade'],
                'matchupFieldGoalsAttempted': record['matchupFieldGoalsAttempted'],
                'matchupFieldGoalsPercentage': record['matchupFieldGoalsPercentage'],
                "shootingFouls" : record["shootingFouls"],
                'matchupFreeThrowsMade' : record['matchupFreeThrowsMade']
            }

            new_rows.append(new_row)

        player_stats_df = pd.DataFrame(new_rows, columns=columns)
        return player_stats_df
    
    def fantasy_df_creation(self):
        fantasy_meta = pd.DataFrame()
        cols = ['Team','MIN','FGM','FGA','FTM','FTA','FG3M','REB','AST','STL','BLK','TOV','PTS']

        for player in self.players:
            df = pd.read_csv(f'players/gamelogs/{player}_log.csv')
            df = df[cols]
            df['Avg Espn Fpts'] = (df['FGM'] * 2) + (df['FGA'] * -1) + df['FTM'] - df['FTA'] + df['FG3M'] + df['REB'] + (df['AST'] * 2) + (df['STL'] * 4) + (df['BLK'] * 4) + (df['TOV'] * -2) + df['PTS']
            
            # Calculate mean, variance, total minutes, and Avg Espn Fpts per minute
            avg_espn_fpts_mean = round(df['Avg Espn Fpts'].mean(),2)
            avg_espn_fpts_variance = round(df['Avg Espn Fpts'].std(),2)
            total_minutes = round(df['MIN'].sum(),2)
            minutes = round(df['MIN'].mean(),2)
            df['Team'] = df.apply(lambda row: row.iloc[0], axis=1)
            avg_espn_fpts_per_minute = round(avg_espn_fpts_mean / minutes,2)
            
            # Create a row for the player in the fantasy_meta DataFrame
            player_meta = pd.DataFrame({
                'Player': [player],
                'Avg Min':[minutes],
                'Avg Espn Fpts Mean': [avg_espn_fpts_mean],
                'Avg Espn Fpts Variance': [avg_espn_fpts_variance],
                'Total Minutes': [total_minutes],
                'Avg Espn Fpts per Minute': [avg_espn_fpts_per_minute]
            })
            
            # Concatenate the player_meta DataFrame to the fantasy_meta DataFrame
     
            fantasy_meta = pd.concat([fantasy_meta, player_meta])

        # Display the fantasy_meta DataFrame
        fantasy_meta.to_csv('fantasy/fantasy_meta.csv',index=False)
        print(fantasy_meta)
        
        
            
    
    def processTeamMeta(self):
        for team_name in self.rosters:
            df = pd.read_csv(f'teams/games/gamelogs/{team_name}.csv')
            
            #calculate team correlations
            col1 = 'pointsPaint'
            col2 = 'PTS'
            correlation = df[col1].corr(df[col2])
            print(f"{team_name} correlation between {col1} and {col2}: {correlation}")
            
            col3 = 'FG3M'
            correlation = df[col3].corr(df[col2])
            
            print(f"{team_name} correlation between {col3} and {col2}: {correlation}\n")
        
                    
    
    
    
    


                    
                    
                    
                     
#DataGetter(execution='runn')
            
            
            
            



#predict totl possesions for a player ina  game, and divide up, 


class Plyrs(DataGetter):
    def __init__(self):
        super().__init__()

    def convert_to_feet_inches(self, height_cm):
        # Convert height from centimeters to feet and inches
        inches = height_cm / 2.54
        feet = int(inches // 12)
        remaining_inches = round(inches % 12, 2)
        return feet, remaining_inches

    def create_players_df(self, team1, team2):
        player_info_dict = {}
        team1 = self.rosters[team1]
        team2 = self.rosters[team2]

        data = {'Position': [], 'Height': [], 'Weight': [], 'Team': [], 'Speed': [], 'Pts_Paint_Min': [], 'Pts_Paint': []}

        for team in [team1, team2]:
            for player, attributes in team.items():
                try:
                    plyr_min = self.players[player]['AVG_MIN']
                    plyr_ht = self.players[player]['HEIGHT']
                    plyr_wt = self.players[player]['WEIGHT']
                    plyr_tm = self.players[player]['TEAM_ABBREVIATION']
                    plyr_pos = self.players[player]['POSITION']
                    plyr_spd = self.players[player]['AVG_SPEED']
                    plyr_ptsmin = self.players[player]['PTS_PAINT_poss']
                    plyr_ptspaint = self.players[player]['PTS_PAINT']

                    if plyr_min >= 20:
                        data['Position'].append(plyr_pos)
                        data['Height'].append(plyr_ht)
                        data['Weight'].append(plyr_wt)
                        data['Team'].append(plyr_tm)
                        data['Speed'].append(plyr_spd)
                        data['Pts_Paint_Min'].append(plyr_ptsmin)
                        data['Pts_Paint'].append(plyr_ptspaint)

                        player_info_dict[player] = {'height': plyr_ht, 'weight': plyr_wt, 'team': plyr_tm,
                                                    'speed': plyr_spd, 'pts_paintmin': plyr_ptsmin,
                                                    'pts_paint': plyr_ptspaint}
                except ValueError:
                    pass
                #except KeyError:
                    #print(player, ' passed')

        players_df = pd.DataFrame(data, index=player_info_dict.keys())
        return players_df
    
   
    
#x = Plyrs()
#print(x.create_players_df('MIA','MIL'))

        