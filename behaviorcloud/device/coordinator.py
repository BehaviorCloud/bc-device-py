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
from .globals import set_config, get_config
# from .device import Device

def flush_print(line):
	print(datetime.datetime.now().isoformat(), line)
	sys.stdout.flush()

def flush_write(message):
	sys.stdout.write(message)
	sys.stdout.flush()

class Coordinator:
	def __init__(self, device_klass):
		self.device_id = None
		self.device_record = None
		self.device = None
		self.device_klass = device_klass
		self.mqtt_client = None
		self.running = False
		self.background_running = False
		self.datasets = []
		self.expirations = {}
		self.expirations_lock = threading.Lock()
		self.ping_response = None
		self.simulated_mode = False
		self.token_stamp = None
		self.parser = argparse.ArgumentParser(description='Run a device firmware for the BehaviorCloud platform.')
		self.parser.add_argument('--host', nargs='?', required=True, help='The hostname of the BehaviorCloud api server. Typically, this is api.behaviorcloud.com.')
		self.parser.add_argument('--token', nargs='?', required=True, help='The JWT token provided by the BehaviorCloud platform.')
		self.parser.add_argument('--id', nargs='?', required=True, help='The device id.')
		self.parser.add_argument('--continuous', action='store_true', help='Will run this device in a continuous collection mode.')
		self.parser.add_argument('--simulated', action='store_true', help='Will have the device stream simulated data.')

	def spawn(self, dataset=None):
		if dataset is None:
			for dataset in self.datasets:
				self.device.start_collection(dataset)
		else:
			self.device.start_collection(dataset)

	def run(self):
		arguments = self.parser.parse_args()
		set_config({
			'HOST': arguments.host,
			'TOKEN': arguments.token,
			'ID': arguments.id,
			'API_VERSION': '1.1',
		})
		self.token_stamp = datetime.datetime.now()
		self.simulated_mode = arguments.simulated

		if not arguments.id:
			flush_print('You must supply an id')
			return
		self.device_id = arguments.id

		# instantiate device class and configure mapping
		if self.device is None:
			self.device = self.device_klass(simulated=self.simulated_mode)
		
		flush_print("Starting {} version {}".format(self.device.firmware_name, self.device.firmware_version))

		api.device_write_map(arguments.id, self.device.get_device_map())

		# retrieve associated realtime datasets
		self.device_record = api.get_device_realtime_datasets(self.device_id)
		self.datasets = self.device_record['realtime_datasets']

		flush_print('Device record loaded: {}'.format(
			self.device_record)
		)
		
		# if any RT datasets are already in collecting mode, spawn immediately
		# and set expirations appropriately.
		for dataset in self.datasets:
			observe_through = (
				dateutil.parser.parse(dataset['observe_through'], ignoretz=True)
				if dataset['observe_through'] is not None
				else None
			)
			flush_print('Dataset {} observe-through: {}'.format(
				dataset['uuid'],
				observe_through)
			)
			if observe_through is not None and observe_through > datetime.datetime.now():
				self.expirations[dataset['uuid']] = observe_through
				self.spawn(dataset)

		# subscribe to RT datasets
		self.connect_mqtt()

		if arguments.continuous:
			self.spawn()
		else:
			self.start()
		
		self.run_background_refresh()
		
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
	
	def run_background_refresh(self):
		if self.background_running:
			return
		self.background_running = True
		flush_print('Starting background ping and token refresh')
		t = threading.Thread(target=self.background_refresh)
		t.start()
	
	def background_refresh(self):
		while (self.background_running):
			self.check_token()
			self.periodic_ping()
			time.sleep(60)
	
	def check_token(self):
		if (datetime.datetime.now() - self.token_stamp) > datetime.timedelta(minutes=10):
			flush_print("Requesting a JWT token refresh")
				
			# refresh token
			try:
				response = api.auth_refresh_token()
				config = get_config()
				config['TOKEN'] = response['token']
				set_config(config)

				self.token_stamp = datetime.datetime.now()

				flush_print("Token refreshed successfully: {}".format(config['TOKEN']))
			except Exception as e:
				flush_print("Failed to refresh token due to {}; will try again in a minute".format(e))

	def periodic_ping(self):
		try:
			response = api.device_ping(self.device_id)
			
			if self.ping_response is not None and self.ping_response != response:
				if ('reboot_request' in self.ping_response and 
					'reboot_request' in response and
					self.ping_response['reboot_request'] != response['reboot_request']):
					# Fall through and allow device to reboot
					flush_print("A reboot request has been received: {}".format(response))
					self.running = False
					self.background_running = False
			
			self.ping_response = response
			flush_print("Sent periodic ping to API")
		except Exception as e:
			flush_print("Failed to send periodic ping to API because {}".format(e))
	
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
			flush_print('Connected to MQTT broker')
			self.mqtt_client.on_message = self.handle_message
			for dataset in self.datasets:
				if dataset['model_updates_topic'] is not None:
					self.mqtt_client.on_subscribe = handle_subscribe
					self.mqtt_client.subscribe(dataset['model_updates_topic'])
					flush_print('Subscribing to realtime updates for {} on {}'.format(
						dataset['uuid'],
						dataset['model_updates_topic']
					))
				
			flush_print('Waiting...')
		
		def handle_disconnect(client, userdata, rc):
			if rc != 0:
				flush_print('Unexpected disconnection from MQTT broker; will reconnect')
		
		def handle_log(client, userdata, level, buf):
			flush_print('MQTT information: {}'.format(buf))

			if 'retrying' in buf:
				flush_print('Trying to refresh device record for fresh MQTT data')
				# try to retrieve updated model updates endpoint
				try:
					self.device_record = api.get_device_realtime_datasets(self.device_id)
					self.datasets = self.device_record['realtime_datasets']
					parsed_url = urlparse(self.datasets[0]['model_updates_endpoint'])
					headers = {
						"Host": "{0:s}".format(parsed_url.netloc),
					}
					self.mqtt_client.ws_set_options(path='?'.join([parsed_url.path, parsed_url.query]), headers=headers)
					flush_print('Retrieved fresh device record and updated MQTT client headers')
				except:
					flush_print('Failed to retrieve device record')
		
		self.mqtt_client.on_connect = handle_connect
		self.mqtt_client.on_disconnect = handle_disconnect
		self.mqtt_client.on_log = handle_log
		flush_print('Trying to connect to realtime endpoint {}'.format(
			self.datasets[0]['model_updates_endpoint'])
		)
		self.mqtt_client.connect(parsed_url.netloc, port=443)
		self.mqtt_client.loop_start()
	
	def handle_message(self, client, userdata, message):
		try:
			dataset_uuid = message.topic.split('/')[-1]
			payload = json.loads(str(message.payload, 'utf-8'))
			flush_print('Model update received for {}: {}'.format(
				dataset_uuid,
				payload)
			)
			if 'observe_through' in payload:
				observe_through = (
					dateutil.parser.parse(payload['observe_through'], ignoretz=True)
					if payload['observe_through'] is not None
					else None
				)
				self.process_expiration(dataset_uuid, observe_through)
		except Exception as e:
			flush_print("Exception in handle_message: {}".format(e))
			
	def find_dataset(self, dataset_uuid):
		for dataset in self.datasets:
			if dataset['uuid'] == dataset_uuid:
				return dataset
		return None
	
	def process_expiration(self, dataset_uuid, expiration):
		self.expirations_lock.acquire()
		dataset = self.find_dataset(dataset_uuid)
		if not dataset_uuid in self.expirations:
			if expiration is not None:
				self.expirations[dataset_uuid] = expiration
				self.spawn(dataset)
		else:
			self.expirations[dataset_uuid] = expiration
			if expiration is None or expiration < datetime.datetime.now():
				del self.expirations[dataset_uuid]
				self.device.stop_collection(dataset)
		self.expirations_lock.release()

	def check_expirations(self):
		to_remove = []
		self.expirations_lock.acquire()
		for dataset_uuid in self.expirations:
			expiration = self.expirations[dataset_uuid]
			if type(expiration) is not datetime.datetime:
				flush_print("Bad expiration state {}".format(self.expirations))
				return
			if expiration < datetime.datetime.now():
				to_remove.append(dataset_uuid)
				self.device.stop_collection(self.find_dataset(dataset_uuid))
		for dataset_uuid in to_remove:
			del self.expirations[dataset_uuid]
		self.expirations_lock.release()
