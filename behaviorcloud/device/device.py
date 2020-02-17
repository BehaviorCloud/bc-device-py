from . import api

VIDEO_SCHEMA = 4

class Device(object):
    firmware_name = 'Undefined'
    firmware_version = 'Unknown'
    config = {}
    simulated = False
    
    def __init__(self, configuration=None, simulated=False):
        if configuration is not None:
            self.config = configuration
        self.simulated = simulated
    
    def get_device_map(self):
        raise Exception('Not implemented')
    
    def start_collection(self, dataset):
        raise Exception('Not implemented')

    def stop_collection(self, dataset):
        raise Exception('Not implemented')
