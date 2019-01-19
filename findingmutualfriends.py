import sys
import re
from pyspark import SparkConf, SparkContext

def get_indirect(data):
	for friend1 in data:
		for friend2 in data:
			if(friend1 != friend2):
				yield((int(friend1), int(friend2)))
 
def get_sort(itr):
	st = sorted(itr,key= lambda cnt:(cnt[1],cnt[0]))[:10]

	return [c for c in st]

conf = SparkConf()
sc = SparkContext(conf=conf)
data = sc.textFile(sys.argv[1])
data = data.map(lambda l: l.split('\t')).map(lambda l: (int(l[0]),l[1].strip() )).cache()
direct = data.flatMapValues(lambda l: [int(k) for k in l.split(',') if l != '' ])
indirect = data.map(lambda l: l[1].split(',')).flatMap(get_indirect).subtract(direct)
pair = indirect.map(lambda l: ((l[0],l[1]),-1)).reduceByKey(lambda v1, v2 : v1 + v2)
cnt = pair.map(lambda m: (m[0][0],(m[0][1],m[1]))).groupByKey()
result = cnt.mapValues(get_sort)
wanted = [924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993]

result = result.filter(lambda m: m[0] in wanted) 
result = result.map(lambda m: (m[0], ",".join([str(r[0]) for r in m[1] ])))
result.saveAsTextFile(sys.argv[2])


sc.stop()
