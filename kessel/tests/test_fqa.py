import os
import unittest
from kessel.kessel import FileQueueAdapter
from kessel.kessel import QueueAdapter
from kessel.kessel import Message
from .unittest_helper import get_unique_base_path


class TestFqaCompliance(unittest.TestCase):

    def queue_adapter_factory(self) -> QueueAdapter:
        options = {}
        options['base_path'] = get_unique_base_path('fqa-compliance')
        return FileQueueAdapter(options=options)

    def test_enqueue_dequeue_message(self):
        qa = self.queue_adapter_factory()
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()

        self.assertEqual(dq_1.payload, 'hello world')
        self.assertEqual(dq_1.id, m1.id)

    def test_dequeue_skip_locked_message_with_1(self):
        qa = self.queue_adapter_factory()
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        self.assertIsNotNone(dq_1)

        dq_2 = qa.dequeue()
        self.assertIsNone(dq_2)

    def test_dequeue_skip_locked_message_with_2(self):
        qa = self.queue_adapter_factory()
        m1 = Message(payload='hello world m1')
        print('enqueue m1')
        m1 = qa.enqueue(m1)
        print('enqueue complete', m1.id)

        m2 = Message(payload='hello world m2')
        print('enqueue m2')
        m2 = qa.enqueue(m2)
        print('enqueue complete', m2.id)

        print('first dequeue, expect m1')
        dq1 = qa.dequeue()
        self.assertEqual(dq1.payload, m1.payload)
        print('second dequeue, expect m2')
        dq2 = qa.dequeue()
        self.assertEqual(dq2.payload, m2.payload)

        # because message is locked, should not be able to dequeue
        dq3 = qa.dequeue()
        self.assertIsNone(dq3)

    def test_commit_removes_message(self):
        qa = self.queue_adapter_factory()
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        self.assertIsNotNone(dq_1)

        # at this point, m1 should exist and be locked

        # because message is locked, it should not be available for dequeue
        dq_2 = qa.dequeue()
        self.assertIsNone(dq_2)

        qa.commit(dq_1)

        # at this point, m1 should not exist

        dq_3 = qa.dequeue()
        self.assertIsNone(dq_3)

    def test_rollback_removes_lock(self):
        qa = self.queue_adapter_factory()
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        self.assertIsNotNone(dq_1)

        # at this point, m1 should exist and be locked

        # because message is locked, it should not be available for dequeue
        dq_2 = qa.dequeue()
        self.assertIsNone(dq_2)

        qa.rollback(dq_1)

        # at this point, m1 should exist and be available for dequeue

        dq_3 = qa.dequeue()
        self.assertIsNotNone(dq_3)


class TestFqa(unittest.TestCase):

    def file_queue_adapter_factory(self, tag: str = 'fqa') -> FileQueueAdapter:
        options = {}
        options['base_path'] = get_unique_base_path(tag)
        return FileQueueAdapter(options=options)

    def test_construct(self):
        fqa = self.file_queue_adapter_factory()
        self.assertIsNotNone(fqa)

    # these test cases test private implementation and will need to be adjusted if impementation changes
    def test_get_lock_file_path(self):
        config = {}
        base_path = get_unique_base_path()
        config['base_path'] = base_path
        expected_message_path = base_path
        expected_lock_path = f'{base_path}/lock'
        message_id = '123'
        expected_message_file_path = f'{base_path}/123.message'
        expected_lock_file_path = f'{base_path}/lock/123.message.lock'

        fqa = FileQueueAdapter(config)
        self.assertEqual(fqa.config.base_path, expected_message_path)
        self.assertEqual(fqa.config.lock_path, expected_lock_path)

        lock_file_path = fqa._get_lock_file_path(message_id=message_id)
        self.assertEqual(lock_file_path, expected_lock_file_path)

        message_file_path = fqa._get_message_file_path(message_id=message_id)
        self.assertEqual(message_file_path, expected_message_file_path)

    def test_json_format(self):
        qa = self.file_queue_adapter_factory()
        qa.config.set('message_format', 'json')

        payload = 'hello world \n'
        payload += 'this statment has a "quote" - watch out \n'
        payload += "this statment has a 'single quote' \n"
        payload += "\t this starts with a tab \n"
        m1 = Message(payload=payload, header='h1')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()

        self.assertEqual(dq_1.payload, payload)
        self.assertEqual(dq_1.id, m1.id)
        self.assertEqual(dq_1.header, m1.header)

    def test_skip_x_messages(self):
        qa = self.file_queue_adapter_factory()
        qa.config.set('skip_random_messages_range', 100)

        m1 = Message(payload='test', header='h1')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()

        self.assertEqual(dq_1.id, m1.id)

    def test_commit_moves_message_to_processed_directory(self):
        qa = self.file_queue_adapter_factory()
        qa.config.set('message_format', 'json')
        qa.config.set('enable_history', True)

        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        qa.commit(dq_1)

        expected_historical_message_file_path = os.path.join(qa.config.history_path, dq_1.id + '.message')
        print('expected_historical_message_file_path: ', expected_historical_message_file_path)
        self.assertTrue(os.path.exists(expected_historical_message_file_path), "Historical message does not exist.")
