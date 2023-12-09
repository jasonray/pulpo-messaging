import os
import unittest
import time
import datetime
from datetime import timedelta
from pulpo_messaging.kessel import FileQueueAdapter
from pulpo_messaging.kessel import QueueAdapter
from pulpo_messaging.kessel import Message
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

    def test_enqueue_dequeue_with_body(self):
        qa = self.queue_adapter_factory()
        m1 = Message(body={'k': 'v'}, payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()

        self.assertEqual(dq_1.payload, 'hello world')
        self.assertEqual(dq_1.get_body_item('k'), 'v')
        self.assertEqual(dq_1.id, m1.id)

    def test_enqueue_dequeue_with_header(self):
        qa = self.queue_adapter_factory()
        m1 = Message(header={'k': 'v'})
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()

        self.assertEqual(dq_1.get_header_item('k'), 'v')
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

    @staticmethod
    def file_queue_adapter_factory(tag: str = 'fqa', additional_options=None) -> FileQueueAdapter:
        options = {}
        options['base_path'] = get_unique_base_path(tag)

        if additional_options:
            for key in additional_options:
                value = additional_options.get(key)
                options[key] = value

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
        qa = TestFqa.file_queue_adapter_factory()
        qa.config.set('message_format', 'json')

        payload = 'hello world \n'
        payload += 'this statment has a "quote" - watch out \n'
        payload += "this statment has a 'single quote' \n"
        payload += "\t this starts with a tab \n"
        m1 = Message(payload=payload, header={'h1'})
        m1 = qa.enqueue(m1)
        self.assertIsNotNone(m1.id)

        dq_1 = qa.dequeue()
        print(f'dq={dq_1}')

        self.assertEqual(dq_1.payload, payload)
        self.assertEqual(dq_1.id, m1.id)
        self.assertEqual(dq_1.header, m1.header)

    def test_skip_x_messages(self):
        qa = TestFqa.file_queue_adapter_factory()
        qa.config.set('skip_random_messages_range', 100)

        m1 = Message(payload='test', header={'h1'})
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()

        self.assertEqual(dq_1.id, m1.id)

    def test_commit_moves_message_to_processed_directory(self):
        qa = TestFqa.file_queue_adapter_factory()
        qa.config.set('message_format', 'json')
        qa.config.set('enable_history', True)

        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        qa.commit(dq_1)

        expected_historical_message_file_path = os.path.join(qa.config.history_success_path, dq_1.id + '.message')
        print('expected_historical_message_file_path: ', expected_historical_message_file_path)
        self.assertTrue(os.path.exists(expected_historical_message_file_path), "Historical message does not exist.")

    def test_delay(self):
        qa = TestFqa.file_queue_adapter_factory()
        m1 = Message(payload='hello world', delay=timedelta(seconds=5))
        m1 = qa.enqueue(m1)

        print('attempt to dequeue, expect message not yet available')
        dq_1 = qa.dequeue()
        self.assertIsNone(dq_1, 'Because message was enqueue with a delay, the message should not have been dequeued at this time')

        print('pause 5 seconds')
        time.sleep(5)

        print('attempt to dequeue, expect message is available')
        dq_2 = qa.dequeue()
        self.assertIsNotNone(dq_2)
        self.assertEqual(dq_2.id, m1.id)

    def test_commit_failure_removes_message(self):
        qa = TestFqa.file_queue_adapter_factory()
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        self.assertIsNotNone(dq_1)
        qa.commit(message=dq_1, is_success=False)

        dq_2 = qa.dequeue()
        self.assertIsNone(dq_2)


class TestFqaMaxAttempts(unittest.TestCase):

    def test_rollback_increments_attempts(self):
        qa = TestFqa.file_queue_adapter_factory()
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        self.assertIsNotNone(dq_1)
        self.assertEqual(dq_1.attempts, 0)

        qa.rollback(dq_1)

        dq_3 = qa.dequeue()
        self.assertIsNotNone(dq_3)
        self.assertEqual(dq_3.attempts, 1)

        qa.rollback(dq_3)

        dq_4 = qa.dequeue()
        self.assertIsNotNone(dq_4)
        self.assertEqual(dq_4.attempts, 2)

    def test_message_exceeds_attempts_unavailable(self):
        qa = TestFqa.file_queue_adapter_factory(additional_options={"max_number_of_attempts": 2})

        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        self.assertIsNotNone(dq_1)
        qa.rollback(dq_1)

        dq_2 = qa.dequeue()
        self.assertIsNotNone(dq_2)
        qa.rollback(dq_2)

        dq_3 = qa.dequeue()
        self.assertIsNone(dq_3)

        # ensure that message is failed
        self.assertEqual(qa.lookup_message_state(m1.id), 'complete.fail')


class TestFqaExpiration(unittest.TestCase):

    def test_skip_expired_message(self):
        expiration_date_in_past = datetime.datetime.strptime("2000-01-01 12:00:00", "%Y-%m-%d %H:%M:%S")
        print(f'{expiration_date_in_past=}')
        qa = TestFqa.file_queue_adapter_factory()
        m1 = Message(payload='hello world', expiration=expiration_date_in_past)
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        self.assertIsNone(dq_1)

        # ensure that message is in history/failed
        self.assertEqual(qa.lookup_message_state(m1.id), 'complete.fail')

    def test_process_message_with_future_expiration(self):
        expiration_date_in_future = datetime.datetime.strptime("3000-01-01 12:00:00", "%Y-%m-%d %H:%M:%S")
        print(f'{expiration_date_in_future=}')
        qa = TestFqa.file_queue_adapter_factory()
        m1 = Message(payload='hello world', expiration=expiration_date_in_future)
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        self.assertIsNotNone(dq_1)

        # ensure that message is locked
        self.assertEqual(qa.lookup_message_state(m1.id), 'lock')

    def test_message_with_no_expiration_is_processed(self):
        qa = TestFqa.file_queue_adapter_factory()
        m1 = Message(payload='hello world', expiration=None)
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        self.assertIsNotNone(dq_1)

        # ensure that message is locked
        self.assertEqual(qa.lookup_message_state(m1.id), 'lock')
