#read data from kafka
#sent to redis 

from kafka import KafkaConsumer
import argparse
import logging
import redis 
import atexit

logging.basicConfig()
logger = logging.getLogger('redis-publisher')
logger.setLevel(logging.DEBUG)

def shutdown_hook(kafka_consumer):
	logger.info('shutdown kafka consumer')
	kafka_consumer.close()

if __name__ == "__main__":

	#set up command line

	parser = argeparse.ArgumentParser()
	parser.add_argument('topic_name',help="kafka topic")
	
	parser.add_argument('kafka_broker',help="kafka broker")
	parser.add_argument('redis_port',help='6379')
	parser.add_argument('redis_host',help='redis local host')
	parser.add_argument('redis_channel',help='redis topic name')

	args= parser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	redis_host = args.redis_host
	redis_port = args.redis_port
	redis_channel = args.redis_channel

	kafka_consumer = KafkaConsumer(
		topic_name,
		boostrap_servers = kafka_broker
	)

	redis_client = redis.StrictRedis(host=redis_host, port = redis_port)


	atexit.register(shutdown_hook, kafka_consumer)
	for msg in kafka_consumer:
		logger.info("receive data from kafka %s" % str(msg))
		redis_client.publish(redis_channel, msg.value)
