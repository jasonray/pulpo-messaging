import argparse
import json
from pulpo_messaging.kessel import PulpoConfig
from pulpo_messaging.kessel import Pulpo
from statman import Statman

parser = argparse.ArgumentParser(description='Kessel-Runner')
parser.add_argument('--config', type=str, help='path to config file')
parser.add_argument('--shutdown_after_number_of_empty_iterations', type=int, help='Support shutdown after x empty queue fetches')
parser.add_argument('--file_queue_adapter.base_path', type=str, help='Directory to use as the private queue repository')
parser.add_argument('--sleep_duration', type=str, help='How long to sleep between queue polls, in seconds')

args = parser.parse_args()

config = PulpoConfig()
if args.config:
    print('load config from file', args.config)
    config = config.fromJsonFile(file_path=args.config)

config.fromArgumentParser(args)
print('kessel config: [{config}]')

kessel = Pulpo(config)
kessel.start()