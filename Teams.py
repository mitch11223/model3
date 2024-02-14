from Team import Team
from Cdata_getter import DataGetter

class Teams(DataGetter):
    def __init__(self, game, away_team, home_team):
        super().__init__(game)
        print('Teams')
        self.home_teamname = home_team
        self.away_teamname = away_team
        self.away_roster = self.rosters[away_team]
        self.home_roster = self.rosters[home_team]
        self.away_team = Team(self.away_roster, 'away',self.away_teamname)
        self.home_team = Team(self.home_roster, 'home',self.home_teamname)
        
        
        self.predictPossessions(self.away_team,self.home_team)
        
    def predictPossessions(away_team,home_team):
        away_tensor = 1
        
        
        
    
    #def match_teams(self):
        #Match(self.away_team,self.home_team) #match the teams for a hypotheitcal game, probbaly create a class for this
    
    '''
    def attributes(self):
        #offense to defense then defense to offense tensors
        
        away_team_tensor = self.away_team.attributes.final_tensor #tensor that will be fitted with the home team tensor to predict the game,
                                                                        #inside a variable, inside a method, inside away Team object
        home_team_tensor
        
     '''   
        
        
        
        #PredictGame() in the game class, by using these variables
        
        