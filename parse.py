from pyspark import SparkContext, SparkConf
import wikiextractor.WikiExtractor as wikix
import sys, os, re
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

def get_links(text):
	p = re.compile('<a href=".+?".*?>')
	a = p.findall(text)

	# Convert anchor tags to just the link
	return map(lambda x: x[9:x.rfind('"')], a)


files = sc.wholeTextFiles('small_pages/*')
converted = files.map(read_files)

# TODO: Get page_id

# Get links
links = converted.map(get_links)
links.first()

# TODO: Toss all tags

"""
word_counts = converted.map(lambda line: line.split(" ")) \
     .filter(lambda w: len(w) >= 3) \
     .map(lambda word: (word, 1)) \
     .reduceByKey(lambda x,y: x+y) \
     .sortBy(lambda x: x[1], False)
"""

# TODO: Need to extract page_id
#page_map = word_counts.map(lambda x: (page_id, list(x))).groupByKey().map(lambda x: (x[0], list(x[1])))
