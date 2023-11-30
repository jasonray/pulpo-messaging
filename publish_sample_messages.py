from kessel.kessel import Message
from kessel.kessel import Kessel
from kessel.kessel import KesselConfig
from statman import Statman
import random
import argparse
import time

parser = argparse.ArgumentParser(
                    prog='publish_sample_messages',
                    description='Published a set of test messages to kessel')

parser.add_argument('--n', dest='number_of_messages',    help='number of messages to publish', type=int, default=100)
parser.add_argument('--t', dest='time', help='if provided, will publish n messages every t seconds', type=int, default=None)
parser.add_argument('--config', type=str, help='path to config file')
args = parser.parse_args()

args = parser.parse_args()
config = None
if args.config:
    print('load config from file', args.config)
    config = KesselConfig(json_file_path=args.config)
else:
    config = KesselConfig()

config.process_args(args)
kessel = Kessel(config)

def create_random_message(payload: str):
    message_type_number = random.randint(1,3)
    message_type = None
    if message_type_number == 1:
        message_type ='echo'
        Statman.gauge(name='publish-sample-messages.published-messages.echo').increment()
    elif message_type_number == 2:
        message_type ='lower'
        Statman.gauge(name='publish-sample-messages.published-messages.lower').increment()
    elif message_type_number == 3:
        message_type ='upper'
        Statman.gauge(name='publish-sample-messages.published-messages.upper').increment()
    return Message(payload=payload, type=message_type)

def publish():
    Statman.stopwatch(name='publish-sample-messages.timing', autostart=True)
    for i in range(0, args.number_of_messages):
        m = create_random_message(payload=f'HellO WorlD {i}')
        kessel.publish(m)
        Statman.gauge(name='publish-sample-messages.published-messages').increment()
    Statman.stopwatch(name='publish-sample-messages.timing').stop()
    
if args.time:
    continue_processing=True
    while continue_processing:
        publish()
        time.sleep(args.time)
else:
    publish()

Statman.stopwatch(name='publish-sample-messages.timing').print()
Statman.report(output_stdout=True)
