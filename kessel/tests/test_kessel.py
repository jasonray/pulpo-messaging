import datetime
import uuid
import os
import unittest
import json
from kessel.kessel import FileQueueAdapter
from kessel.kessel import QueueAdapter
from kessel.kessel import Message
from kessel.kessel import Kessel
from kessel.kessel import Config
from statman import Statman
from statman import Stopwatch


class TestConfig(unittest.TestCase):

    def test_config_shutdown_after_number_of_empty_iterations(self):
        config = Config()
        config.set('shutdown_after_number_of_empty_iterations', 10)
        config.set('queue_adapter_type', 'FileQueueAdapter')
        config.set('file_queue_adapter.base_path','/tmp/kessel/fqa')
        kessel = Kessel(config)
        self.assertEqual(kessel.config.shutdown_after_number_of_empty_iterations,10)
        self.assertEqual(kessel.config.get('file_queue_adapter').get('base_path') , '/tmp/kessel/fqa')

    def test_load_config_from_file(self):
        options = json.load(open('./kessel-config.json', 'rb'))  # pylint: disable=consider-using-with
        print(options)
        kessel = Kessel(options)
        self.assertEqual(kessel.config.shutdown_after_number_of_empty_iterations,7)
        self.assertEqual(kessel.config.get('file_queue_adapter').get('base_path') , '/tmp/kessel/fqa')
