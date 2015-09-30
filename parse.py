import wikiextractor.WikiExtractor as wikix
import sys
from contextlib import contextmanager

f = 'small_pages/page-0001000.xml'

@contextmanager
def redirected(stdout):
	saved_stdout = sys.stdout
	sys.stdout = open(stdout, 'w')
	yield
	sys.stdout = saved_stdout

with redirected(stdout='file.txt'):
	wikix.main(['-l', '-a', f])


# Turn every abnormal character into a space?
