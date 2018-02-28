#write data to any kafka cluster
# write data to any kafka topic
# schedule fetch price from yahoo finance
# configurable stack symbol

# parse command line argument
import argparse
import schedule 
import time 
import logging 
import json

import urllib
import re 

import atexit

from kafka import KafkaProducer
from yahoo_finance import Share

logging.basicConfig()
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

symbol = ''
topic_name = ''
kafka_broker = ''

def shutdown_hook(producer):
	logger.info('closing kafka producer')
	producer.flush(10)
	
	producer.close(10)
	logger.info('kafka producer close')

def get_quote(symbol):
	base_url = 'http://finance.google.com/finance?q='
	content = urllib.urlopen(base_url + symbol).read()
	m = re.search('id="ref_22144_l".*?>(.*?)<', content)
	if m:
		quote = m.group(1)
	else:
		quote = 'no quote available for: ' + symbol
	return quote

def fetch_price_and_send(producer, stock):
	logger.debug('about to fetch price')
	#stock.refresh()
	#price = stock.get_price()
	#trade_time = stock.get_trade_datatime()
	price = get_quote(stock)
	trade_time = int(round(time.time()*1000))
	data={
		'symbol': symbol,
		'last_time': trade_time,
		'price': price
	}

	data = json.dumps(data)
	logger.debug('retrieved stock price %s',data)
	
	try:
		producer.send(topic=topic_name, value=data)
		logger.debug("sent data to kafka %s",data)
	except Expection as e:
		logger.warn('failed to send data to kafka')


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('symbol', help = 'the symbol of the stock')
	parser.add_argument('topic_name', help = 'the name of the topic')
	parser.add_argument('kafka_broker', help = "the location of the kafka")

	args = parser.parse_args()
	symbol = args.symbol
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker

	producer = KafkaProducer(
		bootstrap_servers = kafka_broker
	)

	#stock = get_quote(symbol)

	schedule.every(1).second.do(fetch_price_and_send,producer, symbol)

	atexit.register(shutdown_hook , producer)

	while True:
		schedule.run_pending()
		time.sleep(1)
