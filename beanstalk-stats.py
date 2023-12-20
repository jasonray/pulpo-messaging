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

parser.add_argument('--config', type=str, help='path to config file')
args = parser.parse_args()

config = PulpoConfig().fromJsonFile(args.config).fromArgumentParser(args)
pulpo = Pulpo(config)
pulpo.initialize_queue_adapter()


Statman.report(log_method=logger.info)
