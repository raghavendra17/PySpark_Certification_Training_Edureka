import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
	conf = SparkConf().setAppName("Work Count in Pyspark")
	sc = SparkContext(conf=conf)
	
	textFile = sc.textFile("/user/shubhamsag91edu/data/data/sample.txt")
	rdd1 = textFile.flatMap(lambda line: line.split(" "))
	rdd2 = rdd1.map(lambda word: (word,1))
	rdd3 = rdd2.reduceByKey(lambda a,b: a+b)
	rdd3.saveAsTextFile("/user/shubhamsag91edu/data/output/word_count")

'''import sys
from pyspark import SparkContext, SparkConf

class word():
	if __name__ == "__main__":
		conf = SparkConf().setAppName("Work Count in Pyspark")
		sc = SparkContext(conf=conf)
	
		textFile = sc.textFile("/user/shubhamsag91edu/data/data/sample.txt")
		rdd1 = textFile.flatMap(lambda line: line.split(" "))
		rdd2 = rdd1.map(lambda word: (word,1))
		rdd3 = rdd2.reduceByKey(lambda a,b: a+b)
		rdd3.saveAsTextFile("/user/shubhamsag91edu/data/output/word_count_class")'''

