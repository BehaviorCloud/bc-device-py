import requests
from . import globals


def get_queued_analysis(id):
    response = requests.get(
        globals.get_full_url('queuedanalysis/%s' % (id)),
        headers=globals.get_headers(),
    )
    response.raise_for_status()
    return response.json()

def get_queued_analysis_index():
    response = requests.get(
        globals.get_full_url('queuedanalysis'),
        headers=globals.get_headers(),
    )
    response.raise_for_status()
    return response.json()

def queued_analysis_mark_started(id):
    response = requests.put(
        globals.get_full_url('queuedanalysis/%s/mark_started' % (id)),
        headers=globals.get_headers(),
    )
    response.raise_for_status()
    return response.json()

def queued_analysis_mark_ended(id):
    response = requests.put(
        globals.get_full_url('queuedanalysis/%s/mark_ended' % (id)),
        headers=globals.get_headers(),
    )
    response.raise_for_status()
    return response.json()

def get_dataset(id):
    response = requests.get(
        globals.get_full_url('datasets/%s' % (id)),
        headers=globals.get_headers(),
    )
    response.raise_for_status()
    return response.json()

def dataset_attach_data(id, data, extension):
    response = requests.put(
        globals.get_full_url('datasets/%s/attach_data' % (id)),
        json={'data': data, 'extension': extension},
        headers=globals.get_headers(),
    )
    response.raise_for_status()
    return response.json()

def dataset_get_credentials(id):
    response = requests.get(
        globals.get_full_url('datasets/%s/get_credentials' % (id)),
        headers=globals.get_headers(),
    )
    response.raise_for_status()
    return response.json()

def dataset_update_state(id, fields):
    response = requests.put(
        globals.get_full_url('datasets/%s/update_state' % (id)),
        json=fields,
        headers=globals.get_headers(),
    )
    response.raise_for_status()
    return response.json()

def dataset_abort_upload(id):
    response = requests.put(
        globals.get_full_url('datasets/%s/abort_upload' % (id)),
        headers=globals.get_headers(),
    )
    response.raise_for_status()
    return response.json()

def get_device_realtime_datasets(id):
    response = requests.get(
        globals.get_full_url('devices/%s?with_realtime_datasets=true' % (id)),
        headers=globals.get_headers(),
    )
    response.raise_for_status()
    return response.json()

