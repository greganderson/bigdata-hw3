from pyspark import SparkContext, SparkConf
import wikiextractor.WikiExtractor as wikix
import xml.etree.ElementTree as ET
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
	try:
		s = wikix.main(['-l', '-a', f[0][5:]])
	except:
		return 'Found invalid character'
	return s.encode('utf8')

def get_links(text):
	p = re.compile('<a href=".+?".*?>')
	a = p.findall(text)
	b = map(lambda x: x + '</a>', a)
	c = map(lambda x: ET.fromstring(x).attrib['href'], b)
	return c

def get_page_title(html):
	a = html.split('\n')[0] + '</doc></page>'
	return ET.fromstring(a)[0].attrib['id']


files = sc.wholeTextFiles('small_pages/*')
converted = files.map(read_files)

# Toss all tags
scrubbed_text = converted.map(lambda w: re.sub(r'<.+?>', '', w))

# Get page_id
title_content_map = converted.map(lambda html: (get_page_title(html), html))
# title_content_map.count == 1000

# Get links
links = converted.map(get_links)

word_counts = scrubbed_text.map(lambda line: line.split(" ")) \
     .map(lambda text: filter(lambda w: len(w) >= 3, text)) \
     .map(lambda text: map(lambda word: (word, 1), text)) \
	 .map(lambda text: reduce(pythonReduceByKey, text)) \
     .map(lambda text: text.sortBy(lambda x: x[1], False))
     #.map(lambda text: reduceByKey(lambda x,y: x+y, text)) \


# TODO: Need to extract page_id
#page_map = word_counts.map(lambda x: (page_id, list(x))).groupByKey().map(lambda x: (x[0], list(x[1])))
