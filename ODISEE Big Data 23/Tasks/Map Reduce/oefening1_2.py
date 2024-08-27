# aangepast map-reduce applicatie om het correcte aantal te bekomen.
from mrjob.job import MRJob

class MrLineCount2(MRJob):
    def mapper(self, _, line):
        if line.startswith('id,'):
            return
        yield "Aantal rijen (zonder kolomnamen): ", 1
        
    def reducer(self, key, values):
        yield key, sum(values)
        
if __name__ == '__main__':
    MrLineCount2.run()
