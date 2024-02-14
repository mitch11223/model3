import time
from Cdata_getter import DataGetter
from Day import Day

class RegularSeason(DataGetter):
    def __init__(self):
        super().__init__()
        self.date = time.strftime('%d-%m-%Y')
        print(f'{self.date}\n')
        self.start_day()

    def start_day(self):
        Today = Day(self)
    
    
    

RegularSeason()