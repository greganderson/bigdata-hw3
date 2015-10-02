from pyspark import SparkContext, SparkConf
from collections import Counter
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
	return ET.fromstring(a)[0].attrib['title']

def get_page_id_with_scrubbed(html):
	a = html.split('\n')[0] + '</doc></page>'
	text = re.sub(r'<.+?>', '', html)
	return (ET.fromstring(a)[0].attrib['id'], text)


files = sc.wholeTextFiles('small_pages/*')
converted = files.map(read_files)

# Toss all tags
scrubbed_text = converted.map(get_page_id_with_scrubbed)

# Get page_id
title_content_map = converted.map(lambda html: (get_page_title(html), html))

# Get links
links = converted.map(get_links)

word_counts = scrubbed_text.map(lambda line: (line[0], line[1].split(" "))) \
		.map(lambda text: (text[0], filter(lambda w: len(w) >= 3, text[1]))) \
		.map(lambda text: (text[0], Counter(text[1])))
