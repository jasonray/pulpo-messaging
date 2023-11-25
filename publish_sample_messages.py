from kessel.kessel import Message
from kessel.kessel import Kessel
from kessel.kessel import KesselConfig
from statman import Statman
import argparse

parser = argparse.ArgumentParser(
                    prog='publish_sample_messages',
                    description='Published a set of test messages to kessel')

parser.add_argument('--n', dest='number_of_messages', type=int, default=100)
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

Statman.stopwatch(name='publish-sample-messages.timing', autostart=True)
for i in range(0, args.number_of_messages):
    m = Message(payload=f'hello world {i}')
    kessel.publish(m)
    Statman.gauge(name='publish-sample-messages.published-messages').increment()
Statman.stopwatch(name='publish-sample-messages.timing').stop()

Statman.stopwatch(name='publish-sample-messages.timing').print()
print(Statman.stopwatch(name='publish-sample-messages.published-messages'))
