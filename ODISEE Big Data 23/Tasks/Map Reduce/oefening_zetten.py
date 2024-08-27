
from mrjob.job import MRJob
import csv

class WinCountExtra(MRJob):
    def mapper(self, _, line):
        if line.startswith('id,'):
            return
        fields = line.split(',')
        yield ("totaal", 1)
        

        moves_column = str(fields[12])
        turns_column = int(fields[4])

        zetten_split_count = len(moves_column.split())
        if turns_column == zetten_split_count:
            yield ("overeenkomend", 1)
        if turns_column != zetten_split_count:
            yield ("nietovereenkomend", 1)   
         
    def reducer(self, key, values):
        yield (key, sum(values))
        
if __name__ == '__main__':
    WinCountExtra.run()   
