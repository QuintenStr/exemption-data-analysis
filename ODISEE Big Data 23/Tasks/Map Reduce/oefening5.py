# winst of verlies van openingszetten

from mrjob.job import MRJob
import csv

from mrjob.job import MRJob

class RecordCount(MRJob):

    def mapper(self, _, line):
        # Split the line into name and result
        fields = line.split(',')
        openingset = fields[14]
        winner = fields[6]
        
        if line.startswith('id,'):
            return
        yield openingset, winner
        
    def reducer(self, key, values):
        # Initialize counters
        wins = 0
        losses = 0
        draws = 0

        # Count the number of wins, losses, and draws
        for value in values:
            if value == 'white':
                wins += 1
            elif value == 'black':
                losses += 1
            elif value == 'draw':
                draws += 1

        # Emit the key and the counts
        yield key, (wins, losses, draws)        

if __name__ == '__main__':
    RecordCount.run()
