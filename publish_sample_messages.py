import sys
from loguru import logger
from pulpo_messaging.kessel import Message
from pulpo_messaging.kessel import Pulpo
from pulpo_messaging.kessel import PulpoConfig
from statman import Statman
import random
import argparse
import time
from tqdm import tqdm

parser = argparse.ArgumentParser(
                    prog='publish_sample_messages',
                    description='Published a set of test messages to kessel')

parser.add_argument('--n', dest='number_of_messages',    help='number of messages to publish', type=int, default=100)
parser.add_argument('--t', dest='time', help='if provided, will publish n messages every t seconds', type=int, default=None)
parser.add_argument('--config', type=str, help='path to config file')
parser.add_argument('-v', '--verbose', dest='verbose', action='store_true')
args = parser.parse_args()

if args.verbose:
    logger.remove(0)
    logger.add(sys.stdout, level="TRACE")
else:
    logger.remove(0)
    logger.add(sys.stdout, level="SUCCESS")

config = None
if args.config:
    logger.debug('load config from file', args.config)
    config = PulpoConfig(json_file_path=args.config)
else:
    config = PulpoConfig()

config.fromArgumentParser(args)
kessel = Pulpo(config)


def create_random_message(payload: str):
    request_type_number = random.randint(1,1)
    request_type = None
    if request_type_number == 1:
        request_type ='success'
        Statman.gauge(name='publish-sample-messages.published-messages.success').increment()
    elif request_type_number == 2:
        request_type ='echo'
        Statman.gauge(name='publish-sample-messages.published-messages.echo').increment()
    elif request_type_number == 3:
        request_type ='lower'
        Statman.gauge(name='publish-sample-messages.published-messages.lower').increment()
    elif request_type_number == 4:
        request_type ='upper'
        Statman.gauge(name='publish-sample-messages.published-messages.upper').increment()
    return Message(payload=payload, request_type=request_type)

def publish():
    Statman.stopwatch(name='publish-sample-messages.timing', autostart=True)
    for i in tqdm(range(0, args.number_of_messages)):
        m = create_random_message(payload=f'HellO WorlD {i}')
        kessel.publish(m)
        Statman.gauge(name='publish-sample-messages.published-messages').increment()
    Statman.stopwatch(name='publish-sample-messages.timing').stop()
    
if args.time:
    continue_processing=True
    while continue_processing:
        publish()
        Statman.report(output_stdout=True)
        time.sleep(args.time)
else:
    publish()

logger.success(f'complete, published {Statman.gauge(name="publish-sample-messages.published-messages").value} message(s)')
Statman.report(log_method=logger.success)
