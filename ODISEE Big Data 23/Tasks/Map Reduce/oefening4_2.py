# map-reduce applicatie om de winstkansen van een kleur te bepalen. (ALLEEN ALS VERSCHIL KLEINER IS DAN 100)
from mrjob.job import MRJob
import csv

class WinCountExtra(MRJob):
    def mapper(self, _, line):
        fields = line.split(',')
        
        # tr/except return om fout te voorkomen
        try:
            white_rating = int(fields[9])
            black_rating = int(fields[11])
            if abs(white_rating - black_rating) <= 100:
                column7 = fields[6]
                words = column7.split()
                
                words_to_count = ['black', 'white', 'draw']
                
                for word in words_to_count:
                    if word in words:
                        yield (word, 1)
        except:
            return
        


    def reducer(self, key, values):
        yield (key, sum(values))
        
if __name__ == '__main__':
    WinCountExtra.run()             
