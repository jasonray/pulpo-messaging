import unittest
from pulpo_config import Config
from pulpo_messaging import UpperCaseHandler, LowerCaseHandler, EchoHandler
from .unittest_helper import get_unique_base_path


class TestSampleHandlers(unittest.TestCase):

    def test_upper_handler(self):
        config = Config()
        config.set('destination_directory', get_unique_base_path(tag='upper_handler'))
        handler = UpperCaseHandler(options=config)
        for i in range(0, 2):
            handler.handle(f'{i} Hello World')

    def test_lower_handler(self):
        config = Config()
        config.set('destination_directory', get_unique_base_path(tag='lower_handler'))
        handler = LowerCaseHandler(options=config)
        for i in range(0, 2):
            handler.handle(f'{i} Hello World')

    def test_echo_handler(self):
        config = Config()
        config.set('destination_directory', get_unique_base_path(tag='echo_handler'))
        handler = EchoHandler(options=config)
        for i in range(1, 2):
            handler.handle(f'{i} Hello World')
