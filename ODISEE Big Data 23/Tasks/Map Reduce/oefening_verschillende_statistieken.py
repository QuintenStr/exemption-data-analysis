
from mrjob.job import MRJob
import statistics

class VerschillendeStatistiekenJob(MRJob):
    def mapper(self, _, line):
        if line.startswith('id,'):
            return
        
        fields = line.split(',')

        # AANTAL MINUTEN: MEDIAAN
        # Tijden van 0 of 10000000 zijn berekeningen waar de begin en eind date gelijk zijn of fout. 
        # Daarom yield ik ze niet om een correctere mediaan vervolgens te hebben.

        difference_in_time = float(fields[3]) - float(fields[2])
        
        if(difference_in_time != 0) and (difference_in_time != 10000000):
            aantal_min = difference_in_time / 1000 / 60
            yield "aantal_minuten", aantal_min
            
        # RATED
        if(str(fields[1]).lower() == "true"):
            yield "rated", 1
        else:
            yield "rated", 0
            
        # AANTAL BEURTEN
        yield "aantal_beurten", int(fields[4])
            
        # AANTAL KEER DAT SCHAAK VOORKWAM IN EEN SPEL DAT EINDIGDE IN SCHAAKMAT
        if(str(fields[5]).lower() == "mate"):
            yield "aantal_schaak", int(fields[12].count("+"))
            

    def reducer(self, key, values):
        valuesfix = list(values)
        # AANTAL MINUTEN: MEDIAAN
        if(key == "aantal_minuten"):
            yield "mediaan_minuten", statistics.median(valuesfix)
            
        # RATED: PERCENTAGE
        elif(key == "rated"):
            yield "rated_%", sum(valuesfix) / len(valuesfix) * 100
            
        # BEURTEN EN SCHAAK: MIN, MAX EN GEM
        else:
            yield "minimum_" + key, min(valuesfix)
            yield "maximum_" + key, max(valuesfix)
            yield "average_" + key, sum(valuesfix) / len(valuesfix)

if __name__ == '__main__':
    VerschillendeStatistiekenJob.run()      
