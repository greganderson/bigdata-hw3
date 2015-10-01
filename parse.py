from pyspark import SparkContext, SparkConf
import wikiextractor.WikiExtractor as wikix
import sys, os
from contextlib import contextmanager


### CONFIGURATION ###

conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)


### READ IN FILES ###

def read_files(f):
	return wikix.main(['-l', '-a', f[0]])

f = 'small_pages/page-0001000.xml'
wikix.main(['-l', '-a', f])
files = sc.wholeTextFiles('small_pages/*')
converted = files.map(read_files)

# Just word count the text tag
l = converted.map(lambda line: line.split(" ")) \
     .filter(lambda w: len(w) >= 3) \
     .map(lambda word: (word, 1)) \
     .reduceByKey(lambda x,y: x+y) \
     .sortBy(lambda x: x[1], False)
