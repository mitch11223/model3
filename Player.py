from Cdata_getter import DataGetter
import pandas as pd

class Player(DataGetter):
    def __init__(self, player_name):
        super().__init__()
        self.player_name = player_name
        self.standard_gamelogs = pd.read_csv(f'players/gamelogs/{self.player_name}_log.csv')
        self.offensive_matchups = pd.read_csv(f'players/matchups/data/offense/{self.player_name}_matchups.csv')
        self.defensive_matchups = pd.read_csv(f'players/matchups/data/defense/{self.player_name}_matchups.csv')  # Corrected path
        self.attributes = self.players[self.player_name]
        self.data = {}
        
        self.PartialPossessionTensor  = self.initializePartialPossessionTensor()				#creates tesnor that predicts player matchups
        print(self.PartialPossessionTensor)



    def initializePartialPossessionTensor(self):
        ''' This Method Creates a Players tensor to predict partialPossessions/minutes '''
        
        with open('players/meta/partialPossessionTensorKeys.txt', 'r') as file:
            attribute_list = [line.strip() for line in file]
        partialPossessionTensor = np.array([self.attributes[attribute_list[0]]])
        for attribute in attribute_list[1:]:
            partialPossessionTensor = np.concatenate((partialPossessionTensor, [self.attributes[attribute]]))

        return partialPossessionTensor
    
    def initializePaceTensor(self):
        ''' This method creates the tensor to predict team pace'''
        
        #projected Minutes, player pace, player
        
        with open('players/meta/paceTensorKeys.txt', 'r') as file:
            attribute_list = [line.strip() for line in file]
        paceTensor = np.array([self.attributes[attribute_list[0]]])
        for attribute in attribute_list[1:]:
            paceTensor = np.concatenate((paceTensor, [self.attributes[attribute]]))

        return paceTensor
        
        

    def add_player_variable(self, group, scalar, variable_names):
        try:
            key = f'{group.lower()}_{scalar.lower()}'
            if key not in self.data:
                self.data[key] = {}

            for variable_name in variable_names:
                self.data[key][variable_name] = None  # Placeholder value, you can change this if needed
            return True
        except KeyError:
            print(f"Failed to add {variable_names} for player {self.player_name}")
            return False







from abstract_model import AbstractModel

class PlayerMinutesPrediction(AbstractModel):
    def __init__ (self):
        super().__init__(player_name, features, target, dataset)
        