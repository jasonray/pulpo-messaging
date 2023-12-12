import unittest
from pulpo_messaging.kessel import PulpoConfig
from pulpo_messaging.kessel import Pulpo
from pulpo_messaging.kessel import Config


class TestPulpoConfig(unittest.TestCase):

    def test_config_shutdown_after_number_of_empty_iterations(self):
        config = Config()
        config.set('shutdown_after_number_of_empty_iterations', 10)
        config.set('queue_adapter_type', 'file_queue_adapter')
        kessel = Pulpo(config)
        self.assertEqual(kessel.config.shutdown_after_number_of_empty_iterations, 10)

    def test_config_shutdown_after_number_of_empty_iterations_default(self):
        config = Config()
        kessel = Pulpo(config)
        self.assertEqual(kessel.config.shutdown_after_number_of_empty_iterations, 5)

    def test_config_queue_adapter(self):
        config = Config()
        config.set('queue_adapter_type', 'file_queue_adapter')
        kessel = Pulpo(config)
        self.assertEqual(kessel.config.queue_adapter_type, 'file_queue_adapter')

    def test_config_sleep_duration(self):
        config = Config()
        config.set('sleep_duration', 10)
        kessel = Pulpo(config)
        self.assertEqual(kessel.config.sleep_duration, 10)

    def test_config_sleep_duration_default(self):
        config = Config()
        kessel = Pulpo(config)
        self.assertEqual(kessel.config.sleep_duration, 5)

    def test_config_enable_output_buffering_True(self):
        config = Config()
        config.set('enable_output_buffering', True)
        kessel = Pulpo(config)
        self.assertEqual(kessel.config.enable_output_buffering, True)

    def test_config_enable_output_buffering_true(self):
        config = Config()
        config.set('enable_output_buffering', 'true')
        kessel = Pulpo(config)
        self.assertEqual(kessel.config.enable_output_buffering, True)

    def test_config_enable_output_buffering_1(self):
        config = Config()
        config.set('enable_output_buffering', 1)
        kessel = Pulpo(config)
        self.assertEqual(kessel.config.enable_output_buffering, True)

    def test_config_enable_output_buffering_default(self):
        config = Config()
        kessel = Pulpo(config)
        self.assertEqual(kessel.config.enable_output_buffering, False)

    def test_load_config_from_file_via_constructor(self):
        config = Config(json_file_path='pulpo_messaging/tests/test_config/pulpo-config-fqa.json')
        kessel = Pulpo(config)
        self.assertEqual(kessel.config.shutdown_after_number_of_empty_iterations, 7)
        self.assertEqual(kessel.config.get('file_queue_adapter').get('base_path'), '/tmp/pulpo/fqa')

    def test_load_config_from_file(self):
        config = Config().fromJsonFile(file_path='pulpo_messaging/tests/test_config/pulpo-config-fqa.json')
        kessel = Pulpo(config)
        self.assertEqual(kessel.config.shutdown_after_number_of_empty_iterations, 7)
        self.assertEqual(kessel.config.get('file_queue_adapter').get('base_path'), '/tmp/pulpo/fqa')
