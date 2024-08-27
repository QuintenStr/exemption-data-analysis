# aantal spelers en openingen

from mrjob.job import MRJob

class CountPlayersAndOpenings(MRJob):
        
    def mapper(self, _, line):
        if line.startswith('id,'):
            return
        fields = line.split(',')
        
        player1 = str(fields[8]).strip().lower()
        player2 = str(fields[10]).strip().lower()
        opening = str(fields[14]).strip().lower()
        
        yield "players", player1
        yield "players", player2
        yield "openings", opening
                    
    def reducer(self, key, values):
        unique_values = {}
        
        for value in values:
            unique_values[value] = True
        
        count = len(unique_values)
        
        yield key, count
        
        
if __name__ == '__main__':
    CountPlayersAndOpenings.run()
