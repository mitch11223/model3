from Player import Player
from Cdata_getter import DataGetter
class Team(DataGetter):
    def __init__(self, roster, location,teamname):
        super().__init__()
        self.name = teamname
        self.team_gamelogs = pd.read_csv('teams/games/gamelogs/{self.name}.csv')
        self.roster = roster
        self.location = location
        self.team_players = self.create_player_objects() #*a collection of the individual player objects, that comprise this teams roster*
       
       
    def create_player_objects(self):
        player_objects = []

        for player_name in self.roster:
            try:
                player_attributes = self.players[player_name]
                player_object = Player(player_name)
                player_objects.append(player_object)
            except KeyError:
                print(f"Player {player_name} not found in player data.")

        return player_objects
    

    
    
    
    def attributes(self):
        #will be the collection of player data for each team
        
        #team tensor for this teams poss
        #need for each player, relative to their predicted possessions, take their pace impact of the game
        
        
        '''
        1. Predict each players matchup minutes
        teamPaceDf - player name, projected minutes, pace
        team_pace_rating  = sum(teamPaceDf[pace] * teamPaceDf[projected mintues])
        normalized_pace = team_pace_rating to possessions
        
        2. take the team pace, which is players pace weighted by expected minutes
        3. Convert pace@48 mins to possessions
        '''
        
        
        
            
        categories = ['Overall','Bigs','Wings','Guards']
        '''
        size_weighted 
        offTalent_weighted
        defTalent_weighted
        pace_weighted
        rebpct_weighted
        ptsPaint_weighted
        pointsFastbreak_weighted
        pointsSecondChance_weighted
        pointsFastBreak_weighted
        passes_weighted = self.calculate_team_passes_per_min()
        pts3P_weighted
        
    
        
        size_bigs_weighted = self.calculate_sizes_weighted(self.bigs_dict)
        offTalent_bigs_weighted
        defTalent_bigs_weighted
        pace_bigs_weighted
        rebpct_bigs_weighted
        ptsPaint_bigs_weighted
        pointsFastbreak_bigs_weighted
        pointsSecondChance_bigs_weighted
        pointsFastBreak_bigs_weighted
        pts3P_bigs_weighted
        
        
        size_wings_weighted
        offTalent_wings_weighted
        defTalent_wings_weighted
        pace_wings_weighted
        rebpct_wings_weighted
        ptsPaint_wings_weighted
        pointsFastbreak_wings_weighted
        pointsSecondChance_wings_weighted
        pointsFastBreak_wings_weighted
        pts3P_wings_weighted
        
        
        size_guards_weighted
        offTalent_guards_weighted
        defTalent_guards_weighted
        pace_guards_weighted
        rebpct_guards_weighted
        ptsPaint_guards_weighted
        pointsFastbreak_guards_weighted
        pointsSecondChance_guards_weighted
        pointsFastBreak_guards_weighted
        pts3P_guards_weighted
        '''
        
        
  
        
        
        #These are each teams attributes to be used for the actual predicting of the game
        #teams will have a tensor with this data:
        '''
            W_PCT = Teams Win Percentage zscore :(teams/metadata/team_data_zscore.csv) Key 'Team' column
            E_PACE = Estimated Pace of the active players
            E_REB_PCT = Estimated rebounds % of the active players
            E_OFF_RATING = Estimated offRating of the active players
            E_DEF_RATING = Estimated defRating of the active players
            
            PTS_PAINT = Projected team points in the paint
            PTS_FAST_BREAK = Projected team fast break points
            PTS_OFF_TURNOVERS = Projected points off turnovers
            
            
            #from philly pov: using ATL active player offence, and PHI active player defence,
            
            from the predicted possesions in the game, 
                predict ATL offesnive stats off PHI defensive stats (active players)
                predict PHI offesnive stats off ATL defensive stats (active players)
                
            #team pointsPaint corr and team 3PTs cor 
                
        '''
        
        
        #one row for every player, and their corresponding attributes, to create one big tensor
        
        
        #from 48 minutes utilize both teams PACE values to predict a number of possessions
        #from the number of possessions utilize both teams active players to predict a number of possessions for each player
        #
        
        #player_tensors = all complete player tensors in a list/dict 
        #final_tensor = 
    
    #need to append the player attributes and stuff to them, so we can match playeers for defense and offense, predicting possesions