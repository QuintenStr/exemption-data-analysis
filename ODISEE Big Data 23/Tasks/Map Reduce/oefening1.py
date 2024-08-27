from mrjob.job import MRJob

class MrLineCount(MRJob):
    def mapper(self, _, line):
        yield "Aantal rijen: ", 1
        
    def reducer(self, key, values):
        yield key, sum(values)
        
if __name__ == '__main__':
    MrLineCount.run()
