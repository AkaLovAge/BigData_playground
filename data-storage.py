#read from any kafka 
#write from any cassandra

import argparse
import schedule
import time 
import logging
import atexit
import json 

from kafka import KafkaConsumer
from cassandra.cluster import Cluster

topic_name = ""
kafka_broker = ""
cassandra_broker = ""
keyspace = ""
table = ""

logging.basicConfig()
logger = logging.getLogger('data-storage')
logger.setLevel(logging.DEBUG)

def shutdown_hook(consumer,session):

	logger.info("closing source")

	consumer.close()
	session.shutdown()
	logger.info("release source")

def save_data(stock_data, session):
	try:
		logger.debug('start to save data %s',stock_data)
		parsed = json.loads(stock_data)
		symbol = parsed.get('symbol')
		price = parsed.get('price')
		timestamp = parsed.get('last_time')

		statement = "INSERT INTO %s (symbol, trade_time, price) VALUES ('%s', '%s', '%f')" % (table, symbol, timestamp, price)

		session.execute(statement)

		logger.info('saved data into cassandra')

	except Exception as e:
		logger.debug('cannot save data %s',stock_data)	

if __name__ == "__main__":
	#set up command line argument

	parse = argparse.ArgumentParser()

	parse.add_argument('topic_name', help="the kafka topic name mmto subscribe from")
	parse.add_argument('kafka_broker', help = "kafka broker address")
	parse.add_argument("cassandra_broker", help="cassandra broker address")
	parse.add_argument("keyspace", help = "key space")
	parse.add_argument("table", help="the table in cassandra")

	# -parse argument
	args = parse.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	cassandra_broker = args.cassandra_broker
	keyspace = args.keyspace
	table = args.table

	#create a kafka consumer
	consumer = KafkaConsumer(
		topic_name,
		bootstrap_servers = kafka_broker
	)

	# create a cassanfra session 
	cassandra_cluster = Cluster(
		contact_points = cassandra_broker.split(',')	
	)
	session = cassandra_cluster.connect()

	session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy','replication_factor':'1'}" % keyspace)
	session.set_keyspace(keyspace)
	session.execute("CREATE TABLE IF NOT EXISTS %s (symbol text, trade_time timestamp, price float, PRIMARY KEY (symbol, trade_time))" % table)

	atexit.register(shutdown_hook, consumer, session)

	for msg in consumer:
		logger.debug(msg)
		
		
	
	
			
