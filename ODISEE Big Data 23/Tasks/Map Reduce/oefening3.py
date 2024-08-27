# map-reduce applicatie om het aantal keer dat de verschillende uitkomsten voorkomen te berekenen
from mrjob.job import MRJob
import csv

class WordCount(MRJob):

    def mapper(self, _, line):
        row = next(csv.reader([line]))
        if len(row) > 1:  # Andere methode dan anders maar om ervoor te zorgen dat hij alleen count in de kolom van victory_status, omdat anders de counts correct waren.
            text = row[5]
            words = text.split()
            for word in words:
                if word.lower() in ['draw', 'resign', 'outoftime', 'mate']:
                    yield (word.lower(), 1)

    def combiner(self, word, counts):
        yield (word, sum(counts))

    def reducer(self, word, counts):
        yield (word, sum(counts))

if __name__ == '__main__':
    WordCount.run()      
