import argparse
import json
from kessel.kessel import Message
from kessel.kessel import Kessel
from statman import Statman

parser = argparse.ArgumentParser(description='Kessel-Runner')
parser.add_argument('--config', type=str, help='path to config file')

args = parser.parse_args()

if args.config:
    print('load config from file', args.config)
    options = json.load(open(args.config, "rb"))  # pylint: disable=consider-using-with
    print(options)

kessel = Kessel(options)
# kessel.initialize()