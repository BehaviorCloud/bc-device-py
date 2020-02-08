import argparse
import datetime
import json
import sys
import threading
import time
import paho.mqtt.client as mqtt
import dateutil.parser

# Python 2+3 compatibility
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from . import api
from . import data
from .globals import set_config
# from .device import Device

def flush_print(line):
	print(datetime.datetime.now().isoformat(), line)
	sys.stdout.flush()

def flush_write(message):
	sys.stdout.write(message)
	sys.stdout.flush()

class Coordinator:
	def __init__(self, device_klass):
		self.device_record = None
		self.device = None
		self.device_klass = device_klass
		self.mqtt_client = None
		self.running = False
		self.datasets = []
		self.expirations = {}
		self.expirations_lock = threading.Lock()
		self.parser = argparse.ArgumentParser(description='Run a device firmware for the BehaviorCloud platform.')
		self.parser.add_argument('--host', nargs='?', required=True, help='The hostname of the BehaviorCloud api server. Typically, this is api.behaviorcloud.com.')
		self.parser.add_argument('--token', nargs='?', required=True, help='The JWT token provided by the BehaviorCloud platform.')
		self.parser.add_argument('--id', nargs='?', required=True, help='The device id.')
		self.parser.add_argument('--continuous', action='store_true', help='Will run this device in a continuous collection mode.')

	def spawn(self, dataset_id=None):
		if self.device is None:
			self.device = self.device_klass()
		if dataset_id is None:
			for dataset_id in self.datasets:
				self.device.start_collection(dataset_id)
		else:
			self.device.start_collection(dataset_id)

	def run(self):
		arguments = self.parser.parse_args()
		set_config({
			'HOST': arguments.host,
			'TOKEN': arguments.token,
			'API_VERSION': '1.1',
		})

		if not arguments.id:
			flush_print('You must supply an id')
			return

		# retrieve associated realtime datasets
		self.device_record = api.get_device_realtime_datasets(arguments.id)
		self.datasets = self.device_record['realtime_datasets']

		flush_print('Device record loaded: {}'.format(
			self.device_record)
		)
		
		# if any RT datasets are already in collecting mode, spawn immediately
		# and set expirations appropriately.
		for dataset in self.datasets:
			observe_through = dateutil.parser.parse(dataset['observe_through'], ignoretz=True)
			flush_print('Dataset {} observe-through: {}'.format(
				dataset['id'],
				observe_through)
			)
			if observe_through > datetime.datetime.now():
				self.expirations[dataset['id']] = observe_through
				self.spawn(dataset['id'])

		# subscribe to RT datasets
		self.connect_mqtt()

		if arguments.continuous:
			self.spawn()
		else:
			self.start()
		
	def start(self):
		if self.running:
			return
		self.running = True
		flush_print('Starting realtime dataset monitor')
		t = threading.Thread(target=self.daemon)
		t.start()

	def daemon(self):
		while (self.running):
			self.check_expirations()
			time.sleep(0.01)
	
	def connect_mqtt(self):
		parsed_url = urlparse(self.datasets[0]['model_updates_endpoint'])
		self.mqtt_client = mqtt.Client(transport='websockets')
		self.mqtt_client.tls_set()
		headers = {
			"Host": "{0:s}".format(parsed_url.netloc),
		}
		self.mqtt_client.ws_set_options(path='?'.join([parsed_url.path, parsed_url.query]), headers=headers)
		
		def handle_subscribe(client, userdata, mid, granted_qos):
			flush_print('Subscription success')
		
		def handle_connect(client, data, flags, rc):
			self.mqtt_client.on_message = self.handle_message
			for dataset in self.datasets:
				if dataset['model_updates_topic'] is not None:
					self.mqtt_client.on_subscribe = handle_subscribe
					self.mqtt_client.subscribe(dataset['model_updates_topic'])
					flush_print('Subscribing to realtime updates for {} on {}'.format(
						dataset['id'],
						dataset['model_updates_topic']
					))
				
			flush_print('Waiting...')
		
		self.mqtt_client.on_connect = handle_connect
		flush_print('Trying to connect to realtime endpoint {}'.format(
			self.datasets[0]['model_updates_endpoint'])
		)
		self.mqtt_client.connect(parsed_url.netloc, port=443)
		self.mqtt_client.loop_start()
	
	def handle_message(self, client, userdata, message):
		try:
			dataset_id = message.topic.split('/')[-1]
			payload = json.loads(str(message.payload, 'utf-8'))
			flush_print('Model update received for {}: {}'.format(
				dataset_id,
				payload)
			)
			if 'observe_through' in payload:
				observe_through = dateutil.parser.parse(payload['observe_through'], ignoretz=True)
				self.process_expiration(dataset_id, observe_through)
		except Exception as e:
			flush_print("Exception in handle_message: {}".format(e))
			
	def process_expiration(self, dataset_id, expiration):
		self.expirations_lock.acquire()
		if not dataset_id in self.expirations:
			self.expirations[dataset_id] = expiration
			self.spawn(dataset_id)
		else:
			self.expirations[dataset_id] = expiration
			if expiration < datetime.datetime.now():
				del self.expirations[dataset_id]
				self.device.stop_collection(dataset_id)
		self.expirations_lock.release()

	def check_expirations(self):
		to_remove = []
		self.expirations_lock.acquire()
		for dataset_id in self.expirations:
			expiration = self.expirations[dataset_id]
			if type(expiration) is not datetime.datetime:
				flush_print("Bad expiration state {}".format(self.expirations))
				return
			if expiration < datetime.datetime.now():
				to_remove.append(dataset_id)
				self.device.stop_collection(dataset_id)
		for dataset_id in to_remove:
			del self.expirations[dataset_id]
		self.expirations_lock.release()
