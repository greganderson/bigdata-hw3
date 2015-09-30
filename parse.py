import sys, subprocess

fname = 'small_pages/page-0001000.xml'

proc = subprocess.call([sys.executable, 'wikiextractor/WikiExtractor.py', fname, '-l','-a'], stdout=subprocess.PIPE)
output = proc.communicate()



# Turn every abnormal character into a space?
