import unittest
from pulpo_messaging.kessel import Message
from pulpo_messaging.kessel import Pulpo
from pulpo_messaging.queue_adapter import QueueAdapter
from pulpo_messaging.sample_handlers import AlwaysFailHandler, AlwaysSucceedHandler, AlwaysTransientFailureHandler
from .unittest_helper import get_unique_base_path
from unittest.mock import MagicMock


class TestKessel_HandleMessage(unittest.TestCase):

    def test_single_message_success(self):
        mock_queue_adapter = MagicMock(QueueAdapter)
        pulpo = Pulpo(queue_adapter=mock_queue_adapter)
        pulpo.handler_registry.register('sample', AlwaysSucceedHandler())

        m = Message(message_id=123, payload='hello world', request_type='sample')

        result = pulpo.handle_message(m)

        self.assertTrue(result.isSuccess)
        self.assertTrue(mock_queue_adapter.commit.called)
        self.assertFalse(mock_queue_adapter.rollback.called)

    def test_single_message_failed_fatal(self):
        mock_queue_adapter = MagicMock(QueueAdapter)
        pulpo = Pulpo(queue_adapter=mock_queue_adapter)
        pulpo.handler_registry.register('sample', AlwaysFailHandler())

        m = Message(message_id=123, payload='hello world', request_type='sample')

        result = pulpo.handle_message(m)

        self.assertTrue(result.isFatal)
        self.assertTrue(mock_queue_adapter.commit.called)
        self.assertFalse(mock_queue_adapter.rollback.called)

    def test_single_message_failed_transient(self):
        mock_queue_adapter = MagicMock(QueueAdapter)
        pulpo = Pulpo(queue_adapter=mock_queue_adapter)
        pulpo.handler_registry.register('sample', AlwaysTransientFailureHandler())

        m = Message(message_id=123, payload='hello world', request_type='sample')

        result = pulpo.handle_message(m)

        self.assertTrue(result.isTransient)
        self.assertFalse(mock_queue_adapter.commit.called)
        self.assertTrue(mock_queue_adapter.rollback.called)
