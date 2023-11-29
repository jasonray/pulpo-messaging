import datetime
import uuid
import os
import unittest
import json
from kessel.kessel import FileQueueAdapter, KesselConfig
from kessel.kessel import QueueAdapter
from kessel.kessel import Message
from kessel.kessel import Kessel
from kessel.kessel import Config
from statman import Statman
from statman import Stopwatch


class TestKesselConfig(unittest.TestCase):

    def test_config_shutdown_after_number_of_empty_iterations(self):
        config = Config()
        config.set('shutdown_after_number_of_empty_iterations', 10)
        config.set('queue_adapter_type', 'FileQueueAdapter')
        config.set('file_queue_adapter.base_path', '/tmp/kessel/fqa')
        kessel = Kessel(config)
        self.assertEqual(kessel.config.shutdown_after_number_of_empty_iterations, 10)
        self.assertEqual(kessel.config.get('file_queue_adapter').get('base_path'), '/tmp/kessel/fqa')

    def test_load_config_from_file(self):
        config = Config(json_file_path='./kessel-config.json')
        kessel = Kessel(config)
        self.assertEqual(kessel.config.shutdown_after_number_of_empty_iterations, 7)
        self.assertEqual(kessel.config.get('file_queue_adapter').get('base_path'), '/tmp/kessel/fqa')

    def test_load_config_from_file_2(self):
        config = KesselConfig(json_file_path='./kessel-config.json')
        kessel = Kessel(config)
        self.assertEqual(kessel.config.shutdown_after_number_of_empty_iterations, 7)
        self.assertEqual(kessel.config.get('file_queue_adapter').get('base_path'), '/tmp/kessel/fqa')
