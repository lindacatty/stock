#write data to any kafka cluster
#write data to any kafka topic
#schedule fetch price from yahoo finance
#configurable stock symbol

import argparse
import schedule
import logging
import json
import time

#atexit can be used to register shutdown_hook
import atexit

from kafka import KafkaProducer
from yahoo_finance import Share

logging.basicConfig()
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

topic_name = ''
kafka_broker = ''

def shutdown_hook(producer):
	logger.info('closing kafka producer')
	producer.flush(10)
	producer.close(10)
	logger.info('kafka producer closed')

def fetch_price_and_send(producer, symbol):
	logger.debug('about to fetch price')
	stock = Share(symbol)
	stock.refresh()
	price = stock.get_price()
	trade_time = stock.get_trade_datetime()
	data = {
		'symbol' : symbol,
		'last_trade_time' : trade_time,
		'price': price
	}
	logger.info('retrieved stock price %s', data)

	#encode data format (dict) to convert to json format
	data = json.dumps(data)

	try:
		producer.send(topic=topic_name, value=data)
		logger.debug('sent data to kafka %', data)
	except Exception as e:
		logger.warn('failed to send stock data to kafka')

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('symbol', help='the symbol of the stock')
	parser.add_argument('topic_name', help='the name of the topic')
	parser.add_argument('kafka_broker', help='location of the kafka')

	args = parser.parse_args()
	symbol = args.symbol
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker

	#set up producer
	producer = KafkaProducer(
		#if we have kafka cluster with 1000 nodes, what do we pass to kafka_broker??
		bootstrap_servers = kafka_broker
	)


	#pass producer & stock to fetch_price_and_send every 1 sec
	schedule.every(1).second.do(fetch_price_and_send, producer, symbol)

	atexit.register(shutdown_hook, producer)
	
	while True:
		schedule.run_pending()
		time.sleep(1)