import os
import itertools
import requests
import json
from pandas import json_normalize
from os.path import isabs, join, normpath

def global_REQ():
	MAX_REQUEST_COUNT = 3
	return MAX_REQUEST_COUNT
pwd = "/"


def pretty_print(data):
    """Parse json string into pandas dataframe if possible"""
    try:
        data = json.loads(data)
        for d in data:
            if type(data[d]) is list:
                for file in data[d]:
                    if "file_info" in file:
                        file.pop("file_id")
                        file["file_info"][
                            "size"
                        ] = f"{max(0.001, file['file_info']['size'] / 1024 / 1024):.3f} MB"
                    if "nodes" in file:
                        file["nodes"] = len(file["nodes"])
            else:
                if "file_info" in data[d]:
                    data[d]["file_info"][
                        "size"
                    ] = f"{max(0.001, data[d]['file_info']['size'] / 1024 / 1024):.3f} MB"
            try:
                json_data = json_normalize(data[d])
                if not json_data.empty:
                    print(d, ":")
                    print(json_data.to_string())
                else:
                    print(d, ": empty")
            except:
                print(d, ":", data[d])
    except:
        print("Response JSON parse error")

