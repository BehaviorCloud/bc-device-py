config = {}

def set_config(conf):
    global config
    config = conf

def get_config():
    global config
    return config

def get_full_url(relative_url):
    return '%s/api/%s/%s' % (config['HOST'], config['API_VERSION'], relative_url)

def get_headers():
    return {
		'Accept': 'application/json',
		'Authorization': 'JWT %s' % (config['TOKEN']),
		'Content-Type': 'application/json',
	}
