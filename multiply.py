import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    matrix = record[0]
    if matrix == 'a':
        row = record[1]
        for col in range(0,5):
          mr.emit_intermediate((row,col), record)
    else:
        col = record[2]
        for row in range(0,5):
          mr.emit_intermediate((row,col), record)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    entries = {}
    for v in list_of_values:
      if v[0] == 'a':
          if v[2] not in entries:
              entries[v[2]] = (v[3], 1)
          else: 
              entries[v[2]] = (entries[v[2]][0]*v[3], 2)
      else:
          if v[1] not in entries:
              entries[v[1]] = (v[3], 1)
          else: 
              entries[v[1]] = (entries[v[1]][0]*v[3], 2)
    
    total = 0
    for i in range(0,5):
        if i in entries and entries[i][1] == 2:
            total+=entries[i][0]
        
    mr.emit((key[0], key[1], total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
