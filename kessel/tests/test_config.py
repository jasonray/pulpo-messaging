import unittest
from kessel.kessel import Config


class TestKessel(unittest.TestCase):

    def test_config_get(self):
        options = {}
        options['k'] = 'v'
        config = Config(options=options)
        self.assertEqual(config.get('k'), 'v')

    def test_config_get_empty(self):
        options = {}
        options['k'] = 'v'
        config = Config(options=options)
        self.assertEqual(config.get('k2'), None)

    def test_config_set(self):
        config = Config()
        config.set('k', 'v')
        # print('config', config.__options)
        self.assertEqual(config.get('k'), 'v')

    def test_config_manual_nested(self):
        options = {}
        options['k'] = {'k2': 'v'}
        config = Config(options)
        self.assertEqual(config.get('k').get('k2'), 'v')

    def test_config_set_nested(self):
        config = Config()
        config.set('k.k2', 'v')
        self.assertEqual(config.get('k').get('k2'), 'v')

    def test_config_set_nested(self):
        config = Config()
        config.set('k.k2a.k3', 'v1')
        config.set('k.k2b.k3a', 'v2')
        config.set('k.k2b.k3b', 'v3')
        self.assertEqual(config.get('k').get('k2a').get('k3'), 'v1')
        self.assertEqual(config.get('k').get('k2b').get('k3a'), 'v2')
        self.assertEqual(config.get('k').get('k2b').get('k3b'), 'v3')

        self.assertEqual(config.get('k.k2a.k3'), 'v1')
        self.assertEqual(config.get('k.k2b.k3a'), 'v2')
        self.assertEqual(config.get('k.k2b.k3b'), 'v3')

        self.assertIsNone(config.get('k.k2b.k3d'))
        self.assertIsNone(config.get('k.k2b.k3.x'))
        self.assertIsNone(config.get('k.k2b.x.x'))
