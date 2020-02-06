import json
import requests

def get_data_as_file(url, filename):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                if chunk:
                    f.write(chunk)

def get_data_as_json(url):
    response = requests.get(url)
    return response.json()

def get_data_as_stream(url):
    return requests.get(url, stream=True)

def convert_stream_to_json(r):
    r.iter_content(1000000000)
    return json.loads(r.content)

def convert_stream_to_file(r, local_filename):
    f = open(local_filename, 'wb')
    for chunk in r.iter_content(chunk_size=512 * 1024): 
        if chunk:
            f.write(chunk)
    f.close()
