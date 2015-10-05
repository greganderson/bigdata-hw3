# -*- coding: utf-8 -*-
from pyspark import SparkContext, SparkConf
from collections import Counter
import wikiextractor.WikiExtractor as wikix
import xml.etree.ElementTree as ET
import sys, os, re
from contextlib import contextmanager


### CONFIGURATION ###


'''
if len(sys.argv) != 2:
	print 'Invalid arguments'
	print 'Usage: spark-submit parse.py <search_term>'
	exit(1)

search_term = sys.argv[1]
'''

### READ IN FILES ###

def read_files(f):
    return wikix.main(['-l', '-a', f[0][5:]])

def get_links(text):
    p = re.compile('<a href=".+?".*?>')
    a = p.findall(text)
    b = map(lambda x: x + '</a>', a)
    c = map(lambda x: ET.fromstring(x).attrib['href'], b)
    return c

def get_page_title(html):
	line = html.split('\n')[0]
	b = ''
	try:
		a = line + '</doc></page>'
		b = ET.fromstring(a)[0].attrib['title']
	except:
		a = line + '</doc>'
		b = ET.fromstring(a).attrib['title']
	return b

def get_page_title_with_scrubbed(html):
	line = html.split('\n')[0]
	text = re.sub(r'<.+?>', '', html)
	b = ''
	try:
		a = line + '</doc></page>'
		b = (ET.fromstring(a)[0].attrib['title'], text)
	except:
		a = line + '</doc>'
		b = (ET.fromstring(a).attrib['title'], text)
		with open('error.txt', 'w') as f:
			f.write(line + '</doc>')
	return b

def get_page_title_n_link(html):
    title_and_text = get_page_title_with_scrubbed(html)
    link = get_links(html)
    return (title_and_text[0], link)

def get_top_10(text):
	global word_counts
	counts = word_counts.map(lambda x: (x[0], x[1][text]))
	# Get rid of pages that don't contain the word(s)
	filtered_counts = counts.filter(lambda x: x[1] > 0)
	ranked_results = filtered_counts.join(page_rank)
	sorted_ranked_results = ranked_results.sortBy(lambda x: x[1][1], False)
	return sorted_ranked_results.take(10)

def get_page(title):
	global title_content_map
	return title_content_map.filter(lambda x: x[0] == title).first()[1]


def setup(sc):
	global files
	global converted
	global scrubbed_text
	global title_content_map
	global title_n_links
	global page_rank
	global word_counts

	files = sc.wholeTextFiles('small_pages/*')
	converted = files.map(read_files).cache()

	# Toss all tags
	scrubbed_text = converted.map(get_page_title_with_scrubbed)

	# Get page_id
	title_content_map = converted.map(lambda html: (get_page_title(html), html))

	# Get links
	title_n_links = converted.map(get_page_title_n_link)

	# Compute page rank
	page_rank = title_n_links.flatMapValues(lambda t: t) \
			.map(lambda t: (t[1], 1)) \
			.reduceByKey(lambda x,y: x+y) \
			.map(lambda t: (t[0], t[1] - 1)) \
			.sortBy(lambda x: x[1], False)

	# Compute word count
	word_counts = scrubbed_text.map(lambda line: (line[0], line[1].split(" "))) \
		.map(lambda text: (text[0], filter(lambda w: len(w) >= 3, text[1]))) \
		.map(lambda text: (text[0], Counter(text[1]))).cache()

	# Perform search
	#a = get_top_10(search_term)
	#print a

