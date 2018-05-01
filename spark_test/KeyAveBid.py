import sys
from pyspark import SparkContext
import json

def getAdsBid(line):
	entry = json.loads(line.strip())
	if "keyWords" in entry and "bidPrice" in entry:
		tmp = entry["keyWords"]
		for x in tmp:
			yield (x,(float(entry["bidPrice"]),1.0))
if __name__ == "__main__":
	filePath = sys.argv[1]
	sc = SparkContext(appName = "keyAveBid")
	data = sc.textFile(filePath).flatMap(lambda line: getAdsBid(line)).reduceByKey(lambda v1,v2:(v1[0]+v2[0],v1[1]+v2[1])).map(lambda (x,y):(x,y[0]/y[1])).sortBy(lambda (x,y):y, ascending = False)
	data.saveAsTextFile("Keyword_average_bid")
	sc.stop()
