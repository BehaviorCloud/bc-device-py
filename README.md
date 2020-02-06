# BehaviorCloud Analyzer
#### Python Helper

[![N|Solid](https://behaviorcloud.com/images/section/logo-4ffacbfa.png)](https://behaviorcloud.com/images/section/logo-4ffacbfa.png)

This is a thin package to help you quickly create a BehaviorCloud analyzer. It will handle calls to the BehaviorCloud API server so you don't have to worry about it. This package supports the following features: 
  - Fully managed analyzer creation - you just make the conversion function!
  - One off analysis (pass an "--id" command-line parameter)
  - Daemon analysis (use the "--daemon" flag)
  - Exception handling and automatic upload to Sentry.io

Example Usage:
```python
import json

from behaviorcloud.analyzer.data import convert_stream_to_json
from behaviorcloud.analyzer.coordinator import Coordinator

def convert(source_request, source_settings, settings, source, targets):
    target = targets[0]
    source_data = convert_stream_to_json(source_request)
    analyzed_data = [{processed: True, original: entry} for entry in source_data]
    return [{
        "data": json.dumps(analyzed_data),
        "extension": "json",
        "id": target["id"],
    }]

coordinator = Coordinator(convert)
coordinator.run()
```
