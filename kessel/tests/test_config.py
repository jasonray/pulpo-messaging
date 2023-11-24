import unittest
from kessel.kessel import Config

class TestKessel(unittest.TestCase):

    def test_config_get(self):
        options = {}
        options['k']='v'
        config = Config(options=options)
        self.assertEqual(config.get('k'), 'v')

    def test_config_get_empty(self):
        options = {}
        options['k']='v'
        config = Config(options=options)
        self.assertEqual(config.get('k2'), None)

    def test_config_set(self):
        options = {}
        options['k']='v'
        config = Config()
        config.set('k','v')
        self.assertEqual(config.get('k'), 'v')
