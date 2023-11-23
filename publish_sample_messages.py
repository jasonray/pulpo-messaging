from kessel.kessel import Message
from kessel.kessel import Kessel
from statman import Statman
import argparse

parser = argparse.ArgumentParser(
                    prog='publish_sample_messages',
                    description='Published a set of test messages to kessel')

parser.add_argument('--n', dest='number_of_messages', type=int, default=100)
args = parser.parse_args()


kessel = Kessel()

Statman.stopwatch(name='publish-sample-messages.timing', autostart=True)
for i in range(0, args.number_of_messages):
    m = Message(payload=f'hello world {i}')
    kessel.publish(m)
    Statman.gauge(name='publish-sample-messages.published-messages').increment()
Statman.stopwatch(name='publish-sample-messages.timing').stop()

Statman.stopwatch(name='publish-sample-messages.timing').print()
print(Statman.stopwatch(name='publish-sample-messages.published-messages'))
