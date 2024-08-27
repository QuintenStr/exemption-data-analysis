# map-reduce applicatie om de winstkansen van een kleur te bepalen. (ALLE GAMES)
from mrjob.job import MRJob
import csv

class WinCountAll(MRJob):

    def mapper(self, _, line):
        fields = line.split(',')
        words = fields[6].split()
        
        words_to_count = ['black', 'white', 'draw']
        
        for word in words_to_count:
            if word in words:
                yield (word, 1)         

    def reducer(self, key, values):
        # Sum the counts for each word
        yield (key, sum(values))

if __name__ == '__main__':
    WinCountAll.run()     
