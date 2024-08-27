
#hdfs deel 2

import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes
import statistics

unieke_woorden_set = set()

class MyMapper(api.Mapper):
    def __init__(self, context):
        super(MyMapper, self).__init__(context)
        context.set_status("initializing mapper")
        self.input_words = context.get_counter("WORDCOUNT", "INPUT_WORDS")
        
    def map(self, context):

        # Aantal rijen
        context.emit("Aantal_rijen", 1)

        # Aantal verschillende woorden
        rijen = context.value.split(",")
        woorden = rijen[6].split(" ")
        for woord in woorden:
            context.emit("Woord", woord)
            print(woord)
        
        # Min/max/gem lengte van de tekst 
        context.emit("Lengte", len(woorden))
                
        # Histogram (min aantal woorden is 1 en max aantal woorden is 34, dus we gaan bins maken per stapjes van 5)             
        # De grenzen van de bins zitten in de key, bv bin_1_5 is de bin van 1 tot en met 5.
        if(len(woorden) <= 5):
            context.emit("bin_1_5", 1)
        if(len(woorden) > 5 and len(woorden) <= 10):
            context.emit("bin_6_10", 1)
        if(len(woorden) > 10 and len(woorden) <= 15):
            context.emit("bin_11_15", 1)          
        if(len(woorden) > 15 and len(woorden) <= 20):
            context.emit("bin_16_20", 1) 
        if(len(woorden) > 20 and len(woorden) <= 25):
            context.emit("bin_21_25", 1) 
        if(len(woorden) > 25 and len(woorden) <= 30):
            context.emit("bin_26_30", 1) 
        if(len(woorden) > 30 and len(woorden) <= 35):
            context.emit("bin_31_35", 1)    

        # Lege of whitespace velden tellen
        for item in rijen:
            if item is None or item == "":
                context.emit("Lege_velden", 1)
                
        context.increment_counter(self.input_words, len(woorden))
                
            
            
class MyReducer(api.Reducer):
    def __init__(self, context):
        super(MyReducer, self).__init__(context)
        context.set_status("initializing reducer")
        self.output_words = context.get_counter("WORDCOUNT", "OUTPUT_WORDS")
        
    global unieke_woorden_set
    def reduce(self, context):
         
        # Aantal rijen
        if(context.key == 'Aantal_rijen'):
            context.emit(context.key, sum(context.values))
            
        # Aantal verschillende woorden
        if(context.key == 'Woord'):
            unique_values = {}

            for value in context.values:
                unique_values[value] = True

            count = len(unique_values)
            context.emit("Aantal verschillende woorden", count)

       # Min/max/gem lengte van de text
        if(context.key == 'Lengte'):
            valuesfix = list(context.values)
            context.emit((context.key + '_min'), min(valuesfix))
            context.emit((context.key + '_max'), max(valuesfix))
            context.emit((context.key + '_gem'), statistics.mean(valuesfix))
            
        # Histogram
        if(context.key == "bin_1_5"):
            context.emit("bin_1_5", sum(context.values)) 
        if(context.key == "bin_6_10"):
            context.emit("bin_6_10", sum(context.values))      
        if(context.key == "bin_11_15"):
            context.emit("bin_11_15", sum(context.values))   
        if(context.key == "bin_16_20"):
            context.emit("bin_16_20", sum(context.values))             
        if(context.key == "bin_21_25"):
            context.emit("bin_21_25", sum(context.values))
        if(context.key == "bin_26_30"):
            context.emit("bin_26_30", sum(context.values))
        if(context.key == "bin_31_35"):
            context.emit("bin_31_35", sum(context.values))
            
        # Lege of whitespace velden tellen
        if(context.key == 'Lege_velden'):
            context.emit(context.key, sum(context.values))    
            
        context.increment_counter(self.output_words, 1)

FACTORY = pipes.Factory(MyMapper, reducer_class=MyReducer)        
        
def main():
    pipes.run_task(FACTORY)

if __name__ == '__main__':
    main()
