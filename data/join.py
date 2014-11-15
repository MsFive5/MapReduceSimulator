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
    # key: document identifier
    # value: document contents
    key = record[1]
    record.pop(1)
    value = record
    mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    orderlist = []
    linelist = []
    for v in list_of_values:  
      if v[0] == 'order':
        orderlist.append(v)
      else: 
        linelist.append(v)
    new_list=[]
    for ooo in orderlist:
      for lll in linelist:
        ooo.insert(1,key)
        lll.insert(1,key)
        new_list.append(ooo+lll)
        ooo.pop(1)
        lll.pop(1)
    for nnn in new_list:
      mr.emit(nnn)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
