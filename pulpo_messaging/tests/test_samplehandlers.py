import unittest
from pulpo_config import Config
from pulpo_messaging import UpperCaseHandler, LowerCaseHandler, EchoHandler
from pulpo_messaging.sample_handlers import AlwaysFailHandler, AlwaysSucceedHandler, AlwaysTransientFailureHandler
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

    def test_success(self):
        handler = AlwaysSucceedHandler()
        result = handler.handle(payload='hello world')
        print(f'{result=}')
        self.assertTrue(result.isSuccess)
        self.assertFalse(result.isFatal)
        self.assertFalse(result.isTransient)

    def test_fatal(self):
        handler = AlwaysFailHandler()
        result = handler.handle(payload='hello world')
        self.assertFalse(result.isSuccess)
        self.assertTrue(result.isFatal)
        self.assertFalse(result.isTransient)

    def test_transient(self):
        handler = AlwaysTransientFailureHandler()
        result = handler.handle(payload='hello world')
        self.assertFalse(result.isSuccess)
        self.assertFalse(result.isFatal)
        self.assertTrue(result.isTransient)
