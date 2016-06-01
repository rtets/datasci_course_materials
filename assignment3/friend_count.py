import MapReduce
import sys


mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = 1
    mr.emit_intermediate(key,value)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    value = sum([v for v in list_of_values])
    mr.emit((key, value))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
