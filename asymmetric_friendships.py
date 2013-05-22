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
    A = record[0]
    B = record[1]
    mr.emit_intermediate("%s|||%s" % (A,B),1)
    mr.emit_intermediate("%s|||%s" % (B,A),1)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    if len(list_of_values) < 2:
      A = key.split("|||")[0]
      B = key.split("|||")[1]
      mr.emit((A,B))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
