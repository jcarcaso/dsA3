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
    value = record[1]
    mr.emit_intermediate(value, record)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    lines = []
    orders = []
    for v in list_of_values:
      if v[0] == 'line_item':
        lines.append(v)
      else:
        orders.append(v)
    if len(lines) > 0 and len(orders) > 0:
      for o in orders:
          for l in lines:
            ret = o + l
            mr.emit(ret)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
