import MapReduce
import sys


mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = tuple(sorted(record))
    value = record[0]
    mr.emit_intermediate(key,value)

def reducer(key, list_of_values):
    if len(list_of_values)<2:
        mr.emit((key[0],key[1]))
        mr.emit((key[1],key[0]))
    # if len(list_of_values)<2:
    #     for p in key:
    #         if p not in list_of_values:
    #             mr.emit((p,list_of_values[0]))
    #             break

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
