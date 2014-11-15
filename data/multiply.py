import MapReduce
import sys
import pdb
"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    matrix = record[0]
    value = record[3]
    if matrix == 'a':
      iii = record[1]
      jjj = record[2]
      for kkk in range(5):
        mr.emit_intermediate((iii,kkk), ['a',jjj,value])
    else:
      jjj = record[1]
      kkk = record[2]
      for iii in range(5):
        mr.emit_intermediate((iii,kkk),['b',jjj,value])


def reducer(key, list_of_values):
    total = 0
    aaa=[]
    bbb=[]
    for v in list_of_values:
      if v[0] == 'a':
        aaa.append((v[1], v[2]))
      else:
        bbb.append((v[1],v[2]))
    for (jj,va) in aaa:
      for (ll,vb) in bbb:
        if jj == ll:
          total = total + va * vb
    mr.emit((key[0],key[1],total))
    

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
