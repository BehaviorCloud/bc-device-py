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

# def dataset_get_credentials(id):
#     response = requests.get(
#         globals.get_full_url('datasets/%s/get_credentials' % (id)),
#         headers=globals.get_headers(),
#     )
#     response.raise_for_status()
#     return response.json()

def device_generate_dataset_credentials(device_id, ds_id):
        response = requests.post(
            globals.get_full_url('devices/%s/generate_dataset_credentials' % (device_id)),
            json={'dataset_id': ds_id},
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

def device_write_map(id, device_map):
    payload = {
        "devices": [{"realtime_id": device, "schema_id": device_map[device]} for device in device_map]
    }
    response = requests.put(
        globals.get_full_url('devices/%s/map' % (id)),
        json=payload,
        headers=globals.get_headers(),
    )
    response.raise_for_status()
    return response.json()

def device_ping(id):
    payload = {}
    response = requests.put(
        globals.get_full_url('devices/%s/ping' % (id)),
        json=payload,
        headers=globals.get_headers(),
    )
    response.raise_for_status()
    return response.json()

def auth_refresh_token():
    payload = {
        'token': globals.get_config()['TOKEN'],
    }
    response = requests.post(
        globals.get_full_url('device-token-refresh/'),
        json=payload,
        headers=globals.get_headers(),
    )
    response.raise_for_status()
    return response.json()
