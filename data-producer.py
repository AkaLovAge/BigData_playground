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
import requests

from bs4 import BeautifulSoup
import urllib
import re 

import atexit

from kafka import KafkaProducer
from yahoo_finance import Share

from py_zipkin.zipkin import zipkin_span
from py_zipkin.thread_local import get_zipkin_attrs
from py_zipkin.util import generate_random_64bit_string

logging.basicConfig()
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

symbol = ''
topic_name = ''
kafka_broker = ''

def http_transport_handler(span):
	requests.post("http://localhost:9411/api/v1/spans",data=span,headers={'Content-Type':'application/x-thrift'})

def shutdown_hook(producer):
	logger.info('closing kafka producer')
	producer.flush(10)
	
	producer.close(10)
	logger.info('kafka producer close')

#enrichment
def enrich_with_zipkin_data(data):
	zipkin_attr = get_zipkin_attrs()
	data['trace_id'] = zipkin_attr.trace_id
	data['parent_span_id'] = zipkin_attr.parent_span_id
	data['is_sampled'] = True if zipkin_attr.is_sampled else False
	
	return data

@zipkin_span(service_name='data-producer',span_name='fetch_price')
def fetch_price(symbol):
	logger.debug('about to fetch price')
	base_url = 'http://finance.google.com/finance?q='
	content = BeautifulSoup(urllib.urlopen(base_url + symbol).read(),"html.parser")
	m = content.find("span",{"class":"pr"})
	price = 0

	if m:
		m = m.find("span")
		price = float(m.text.replace(',',''))
	else:
		logger.warn("cannot get price from google finance")
		return None

	if price:
		trade_time = int(round(time.time()*1000))
		data = {
			'symbol': symbol,
			'last_time': trade_time,
			'price': price
		}
		data = enrich_with_zipkin_data(data)
		data = json.dumps(data)
		logger.debug('retrieved stock price %s',data)
		return data 

@zipkin_span(service_name='data-producer', span_name='send_to_kafka')
def send2_kafka(producer,data):
	try:
		producer.send(topic = topic_name, value = data)
		logger.debug("sent data to kafka %s", data)
	except Exception as e:
		logger.warn("failed to send data to kafka")

def fetch_price_and_send(producer, stock):

	with zipkin_span(service_name='data-producer',span_name="fetch-price",transport_handler=http_transport_handler, sample_rate=100.0):
		data = fetch_price(stock)	
		if data:
			send2_kafka(producer, data)

	
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
