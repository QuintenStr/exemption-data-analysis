
import pydoop.mapreduce.api as api
import pydoop.mapreduce.pipes as pipes

class MyMapper(api.Mapper):            
    # Dan aantal kolommen per rij mappen    
    def map(self, context):
        column_amount = len(context.value.split(","))
        context.emit("aantal_kolommen", column_amount)
    
class MyReducer(api.Reducer):
    # Alleen aantal_kolommen reducen met max()
    def reduce(self, context):
        if(context.key == 'aantal_kolommen'):
            context.emit(context.key, max(context.values))

FACTORY = pipes.Factory(MyMapper, reducer_class=MyReducer)        
        
def main():
    pipes.run_task(FACTORY)

if __name__ == '__main__':
    main()
