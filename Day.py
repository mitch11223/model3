from Game import Game
from Cdata_getter import DataGetter
class Day(DataGetter):
    def __init__(self, regular_season):
        super().__init__()
        self.regular_season = regular_season
        self.date = self.regular_season.date  # Access the date from RegularSeason instance
        print(f"\tThere are {len(self.todays_games)} games today\n")
        self.games = self.get_games()
        self.game_report

    def get_games(self):
        games_list = {}
        for game in self.todays_games:
            Game_ = Game(self, game)
            game_id = Game_.game_id
            games_list[game_id] = Game_
        
        return games_list
            
    
    
    #def game_report(self)
        