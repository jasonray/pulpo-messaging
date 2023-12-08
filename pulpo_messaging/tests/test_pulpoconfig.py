import unittest
from pulpo_messaging.kessel import PulpoConfig
from pulpo_messaging.kessel import Pulpo
from pulpo_messaging.kessel import Config


class TestPulpoConfig(unittest.TestCase):

    def test_config_shutdown_after_number_of_empty_iterations(self):
        config = Config()
        config.set('shutdown_after_number_of_empty_iterations', 10)
        config.set('queue_adapter_type', 'FileQueueAdapter')
        config.set('file_queue_adapter.base_path', '/tmp/pulpo/fqa')
        kessel = Pulpo(config)
        self.assertEqual(kessel.config.shutdown_after_number_of_empty_iterations, 10)
        self.assertEqual(kessel.config.get('file_queue_adapter').get('base_path'), '/tmp/pulpo/fqa')

    def test_load_config_from_file(self):
        config = Config(json_file_path='./pulpo-config.json')
        kessel = Pulpo(config)
        self.assertEqual(kessel.config.shutdown_after_number_of_empty_iterations, 7)
        self.assertEqual(kessel.config.get('file_queue_adapter').get('base_path'), '/tmp/pulpo/fqa')

    def test_load_config_from_file_2(self):
        config = PulpoConfig(json_file_path='./pulpo-config.json')
        kessel = Pulpo(config)
        self.assertEqual(kessel.config.shutdown_after_number_of_empty_iterations, 7)
        self.assertEqual(kessel.config.get('file_queue_adapter').get('base_path'), '/tmp/pulpo/fqa')
