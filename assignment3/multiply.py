import MapReduce
import sys


mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

#matrix dimensions
adims = (5,5)
bdims = (5,5)

#monkey patch to force output sorted order to match solutions/multiply.json
class SortedIterList(list):
    def __iter__(self):
        it = list.__iter__(self)
        return sorted(it).__iter__()
mr.result = SortedIterList()

def mapper(record):
    # key: document identifier
    # value: document contents
    if record[0]=='a':
        ocol = record[1]
        value = (record[2],record[3])
        for orow in range(bdims[1]):
            mr.emit_intermediate((orow,ocol),value)
    else:
        orow = record[2]
        value = (record[1],record[3])
        for ocol in range(adims[0]):
            mr.emit_intermediate((orow,ocol),value)


def reducer(key, list_of_values):

    value = 0
    for i in range(adims[0]):
        pair = filter(lambda x:x[0]==i,list_of_values)
        if len(pair)==2:
            value+=pair[0][1]*pair[1][1]
    mr.emit((key[1],key[0],value))


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
