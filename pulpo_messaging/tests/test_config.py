import unittest
from pulpo_config import Config


class TestConfig(unittest.TestCase):

    def test_config_get(self):
        options = {}
        options['k'] = 'v'
        config = Config(options=options)
        self.assertEqual(config.get('k'), 'v')

    def test_config_set(self):
        config = Config()
        config.set('k', 'v')
        self.assertEqual(config.get('k'), 'v')

    def test_config_set_nested(self):
        config = Config()
        config.set('k.k2a.k3', 'v1')
        self.assertEqual(config.get('k').get('k2a').get('k3'), 'v1')
        self.assertEqual(config.get('k.k2a.k3'), 'v1')

    def test_load_config_from_file(self):
        config = Config(json_file_path='./pulpo-config.json')
        self.assertEqual(config.get('shutdown_after_number_of_empty_iterations'), 7)
        self.assertEqual(config.get('file_queue_adapter.base_path'), '/tmp/pulpo/fqa')
        self.assertEqual(config.get('file_queue_adapter').get('base_path'), '/tmp/pulpo/fqa')

    def test_load_config_from_file_then_apply_args(self):
        config = Config(json_file_path='./pulpo-config.json')
        self.assertEqual(config.get('shutdown_after_number_of_empty_iterations'), 7)
        self.assertEqual(config.get('file_queue_adapter.base_path'), '/tmp/pulpo/fqa')

        args = {}
        args['shutdown_after_number_of_empty_iterations'] = 10
        args['file_queue_adapter.base_path'] = '/t/k/fqa'

        config.process_args(args)

        self.assertEqual(config.get('file_queue_adapter.base_path'), '/t/k/fqa')
        self.assertEqual(config.get('file_queue_adapter').get('base_path'), '/t/k/fqa')
