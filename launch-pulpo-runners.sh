#!/bin/bash

truncate -s 0 /tmp/kessel/kessel.log

python3 kessel-runner.py --config=kessel-config.json >> /tmp/kessel/kessel.log 2>&1 &
python3 kessel-runner.py --config=kessel-config.json >> /tmp/kessel/kessel.log 2>&1 &
python3 kessel-runner.py --config=kessel-config.json >> /tmp/kessel/kessel.log 2>&1 &
python3 kessel-runner.py --config=kessel-config.json >> /tmp/kessel/kessel.log 2>&1 &
python3 kessel-runner.py --config=kessel-config.json >> /tmp/kessel/kessel.log 2>&1 &
