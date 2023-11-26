import argparse
import json
from kessel.kessel import KesselConfig
from kessel.kessel import Kessel
from statman import Statman

parser = argparse.ArgumentParser(description='Kessel-Runner')
parser.add_argument('--config', type=str, help='path to config file')
parser.add_argument('--shutdown_after_number_of_empty_iterations', type=int, help='Support shutdown after x empty queue fetches')
parser.add_argument('--file_queue_adapter.base_path', type=str, help='Directory to use as the private queue repository')
parser.add_argument('--sleep_duration', type=str, help='How long to sleep between queue polls, in seconds')

args = parser.parse_args()

config = None
if args.config:
    print('load config from file', args.config)
    config = KesselConfig(json_file_path=args.config)
else:
    config = KesselConfig()

config.process_args(args)
print('kessel config: [{config}]')

kessel = Kessel(config)
kessel.initialize()