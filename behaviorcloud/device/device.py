class Device(object):
    config = {}
    def __init__(self, configuration=None):
        if configuration is not None:
            self.config = configuration
    
    def start_collection(self):
        raise Exception('Not implemented')

    def stop_collection(self):
        raise Exception('Not implemented')
