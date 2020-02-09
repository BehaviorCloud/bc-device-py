class Device(object):
    config = {}
    simulated = False
    
    def __init__(self, configuration=None, simulated=False):
        if configuration is not None:
            self.config = configuration
        self.simulated = simulated
    
    def start_collection(self, dataset):
        raise Exception('Not implemented')

    def stop_collection(self, dataset):
        raise Exception('Not implemented')
