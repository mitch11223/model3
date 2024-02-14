from Teams import Teams
from Cdata_getter import DataGetter
import pandas as pd

class Game(DataGetter):
    def __init__(self, day, game_data):
        super().__init__(day)
        self.game_data = game_data
        self.set_game_attributes()
        print(f'\n\tInitializing {self.away_name} @ {self.home_name}')
        self.teams = Teams(self, self.away_name, self.home_name)
        
        self.create_game()
            
    def set_game_attributes(self):
        self.game_id = self.game_data['gameId']
        self.away_name = self.game_data['gameCode'][-6:-3]
        self.away_teamid = self.game_data['awayTeam']['teamId']
        self.home_name = self.game_data['gameCode'][-3:]
        self.home_teamid = self.game_data['homeTeam']['teamId']

        game_playerset = pd.DataFrame()
            #one row for every player, and their corresponding attributes, to create one big tensor
                
    #def create_game(self):
        
          
    #def track_actual_game
        
        
    #def compare_preds_to_actual
    
 
