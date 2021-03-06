#!/usr/bin/env python3

import json

with open("config.json") as f:
    config = json.load(f)

def get(key):
    return config.get(key)
