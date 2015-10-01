from pyspark import SparkContext, SparkConf

### CONFIGURATION ###                                                                                                          
conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

lines = sc.textFile("textFile.txt")

l = lines.flatMap(lambda line: line.split(" ")) \
     .filter(lambda w: len(w) > 0) \
     .map(lambda word: (word, 1)) \
     .reduceByKey(lambda x,y: x+y) \
     .sortBy(lambda x: x[1], False)

l = l.collect()
with open('result.txt', 'w') as fl:
    fl.write(str(l))
