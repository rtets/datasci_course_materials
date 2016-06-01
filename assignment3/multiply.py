import MapReduce
import sys


mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

adims = (5,5)
bdims = (5,5)

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
