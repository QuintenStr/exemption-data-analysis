from mrjob.job import MRJob

class CountColumns(MRJob):

    def mapper(self, _, line):
        fields = line.split(',')
        yield "aantal_kolommen", len(fields)
        
    def reducer(self, key, values):
        yield key, max(values)
if __name__ == '__main__':
    CountColumns.run()
