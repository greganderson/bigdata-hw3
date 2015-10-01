#import sys
from pyspark import SparkContext, SparkConf
import re

### CONFIGURATION ###
conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

lines = sc.textFile("file.txt")

ls = lines.collect();

#id = ""

for line in ls:
    if "id" in line:
        matchObj = re.match( r'id =', line, re.M|re.I)
        if matchObj:
            print "\n\n\n\n\nmatchObj.group(1) : ", matchObj.group(0)
            print "\n\n\n\n\nmatchObj.group(1) : ", matchObj.group(1)
        else:
            print "No match."


#l = lines.map(get_id)

'''
l = lines.flatMap(lambda line: line.split(" ")) \
     .filter(lambda w: len(w) > 0) \
     .map(lambda word: (word, 1)) \
     .reduceByKey(lambda x,y: x+y) \
     .sortBy(lambda x: x[1], False) 
'''

l = l.collect()
with open('result.txt', 'w') as fl:
    fl.write(str(l))
