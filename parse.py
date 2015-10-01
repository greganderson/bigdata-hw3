#from pyspark import SparkContext, SparkConf
import wikiextractor.WikiExtractor as wikix
import sys, os
from contextlib import contextmanager


### CONFIGURATION ###

"""
conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)
"""


### READ IN FILES ###


def read_files(f):
	wikix.main(['-l', '-a', f[0]])


f = 'small_pages/page-0001000.xml'
s = wikix.main(['-l', '-a', f])
#files = sc.wholeTextFiles('small_pages/*')
#converted = files.map(read_files)
#converted.first()


# Turn every abnormal character into a space?
