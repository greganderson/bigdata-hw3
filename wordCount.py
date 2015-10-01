#import sys
from pyspark import SparkContext, SparkConf
import re

### CONFIGURATION ###
conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

f = sc.textFile('file.txt')

lines = f.take(5)

s = ''
for line in lines:
	s += line

start = s.find('id="')
s = s[start+4:]
print s[:s.find('"')]
