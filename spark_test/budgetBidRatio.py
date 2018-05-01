import sys
from pyspark import SparkContext
import json

def adsProcess(line):
	entry = json.loads(line.strip())
	if "adId" in entry and "campaignId" in entry and "bidPrice" in entry:
		return (str(entry["campaignId"]), (str(entry["adId"]), float(entry["bidPrice"])))

def budgetProcess(line):
	entry = json.loads(line.strip())
	if "budget" in entry and "campaignId" in entry:
		return (str(entry["campaignId"]), float(entry["budget"]))

if __name__ == "__main__":
	adsFile = sys.argv[1]
	budgetFile = sys.argv[2]
	sc = SparkContext(appName = "budgetBidRatio")
	
	ads = sc.textFile(adsFile).map(lambda line: adsProcess(line))

	budget = sc.textFile(budgetFile).map(lambda line: budgetProcess(line))

	data = ads.join(budget).map(lambda (campaignId,(ad, bud)): (ad[0], bud/ad[1])).sortBy(lambda (x,y): y, ascending = False)

	data.saveAsTextFile("budgetBidRatio")
	sc.stop()
