import datetime
import uuid
import os
import unittest
from kessel.kessel import FileQueueAdapter
from kessel.kessel import QueueAdapter
from kessel.kessel import Message
import time
import inspect

_kessel_root_directory = '/tmp/kessel/unit-test'
_timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


def get_unique_base_path(tag: str = None):
    path = _kessel_root_directory
    path = os.path.join(path, _timestamp)
    if tag:
        path = os.path.join(path, tag)

    path = os.path.join(path, str(uuid.uuid4()))
    return path


class TestFqaCompliance(unittest.TestCase):

    def queue_adapter_factory(self) -> QueueAdapter:
        return FileQueueAdapter(base_path=get_unique_base_path('fqa-compliance'))

    def test_enqueue_dequeue_message(self):
        qa = self.queue_adapter_factory()
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue_next()

        self.assertEqual(dq_1.payload, 'hello world')
        self.assertEqual(dq_1.id, m1.id)

    def test_dequeue_skip_locked_message_with_1(self):
        qa = self.queue_adapter_factory()
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue_next()
        self.assertIsNotNone(dq_1)

        dq_2 = qa.dequeue_next()
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
        dq1 = qa.dequeue_next()
        self.assertEqual(dq1.payload, m1.payload)
        print('second dequeue, expect m2')
        dq2 = qa.dequeue_next()
        self.assertEqual(dq2.payload, m2.payload)

        # because message is locked, should not be able to dequeue
        dq3 = qa.dequeue_next()
        self.assertIsNone(dq3)

    def test_commit_removes_message(self):
        qa = self.queue_adapter_factory()
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue_next()
        self.assertIsNotNone(dq_1)

        # at this point, m1 should exist and be locked

        # because message is locked, it should not be available for dequeue
        dq_2 = qa.dequeue_next()
        self.assertIsNone(dq_2)

        qa.commit(dq_1)

        # at this point, m1 should not exist

        dq_3 = qa.dequeue_next()
        self.assertIsNone(dq_3)

    def test_rollback_removes_lock(self):
        qa = self.queue_adapter_factory()
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue_next()
        self.assertIsNotNone(dq_1)

        # at this point, m1 should exist and be locked

        # because message is locked, it should not be available for dequeue
        dq_2 = qa.dequeue_next()
        self.assertIsNone(dq_2)

        qa.rollback(dq_1)

        # at this point, m1 should exist and be available for dequeue

        dq_3 = qa.dequeue_next()
        self.assertIsNotNone(dq_3)


class TestFqa(unittest.TestCase):

    def file_queue_adapter_factory(self, tag: str = 'fqa') -> FileQueueAdapter:
        return FileQueueAdapter(base_path=get_unique_base_path(tag))

    def test_construct(self):
        fqa = self.file_queue_adapter_factory()
        self.assertIsNotNone(fqa)

    def test_publish_message(self):
        fqa = self.file_queue_adapter_factory()
        m = Message(payload='hello world')
        m = fqa.enqueue(m)

        self.assertTrue(fqa._does_message_exist(m.id))

    def test_dequeue_message(self):
        fqa = self.file_queue_adapter_factory()
        m1 = Message(payload='hello world')
        m1 = fqa.enqueue(m1)

        dq_1 = fqa.dequeue_next()
        self.assertEqual(dq_1.payload, 'hello world')
        self.assertEqual(dq_1.id, m1.id)

        # messsage should exist, lock should exist
        self.assertTrue(fqa._does_message_exist(m1.id))
        self.assertTrue(fqa._does_lock_exist(m1.id))

    def test_commit_removes_message(self):
        fqa = self.file_queue_adapter_factory()
        m1 = Message(payload='hello world')
        m1 = fqa.enqueue(m1)

        # messsage should exist, lock should not exist
        self.assertTrue(fqa._does_message_exist(m1.id))
        self.assertFalse(fqa._does_lock_exist(m1.id))

        dq_1 = fqa.dequeue_next()
        # messsage should exist, lock should exist
        self.assertTrue(fqa._does_message_exist(m1.id))
        self.assertTrue(fqa._does_lock_exist(m1.id))

        fqa.commit(dq_1)

        # messsage should not exist, lock not should exist
        self.assertFalse(fqa._does_message_exist(m1.id))
        self.assertFalse(fqa._does_lock_exist(m1.id))

    def test_rollback_removes_lock(self):
        fqa = self.file_queue_adapter_factory()
        m1 = Message(payload='hello world')
        m1 = fqa.enqueue(m1)

        # messsage should exist, lock should not exist
        self.assertTrue(fqa._does_message_exist(m1.id))
        self.assertFalse(fqa._does_lock_exist(m1.id))

        dq_1 = fqa.dequeue_next()
        # messsage should exist, lock should exist
        self.assertTrue(fqa._does_message_exist(m1.id))
        self.assertTrue(fqa._does_lock_exist(m1.id))

        fqa.rollback(dq_1)

        # messsage should exist, lock not should exist
        self.assertTrue(fqa._does_message_exist(m1.id))
        self.assertFalse(fqa._does_lock_exist(m1.id))

    def get_message_content(self, path_file):
        with open(file=path_file, encoding="utf-8", mode='r') as f:
            contents = f.read()
        return contents

    # def get_unique_base_path(self, tag: str = None):
    #     path = self._kessel_directory
    #     print('get_unique_base_path p1', path)
    #     if tag:
    #         path = os.path.join(path, tag)
    #     print('get_unique_base_path p2', path)
    #     path = os.path.join(path, str(uuid.uuid4()))
    #     print('get_unique_base_path p3', path)
    #     return path

    # def test_enqueue_dequeue_x100(self):
    #     self.run_pref_test(100,False)

    # def test_enqueue_dequeue_x1000(self):
    #     self.run_pref_test(1000,False)

    # def test_enqueue_dequeue_x10000(self):
    #     self.run_pref_test(1000,False)

    # def test_enqueue_dequeue_with_commit_x100(self):
    #     self.run_pref_test(100,True)

    # def test_enqueue_dequeue_with_commit_x1000(self):
    #     self.run_pref_test(1000,True)

    # def test_enqueue_dequeue_with_commit_x10000(self):
    #     self.run_pref_test(10000,True)


    # def run_pref_test(self, number_of_iterations:int, commit_messages: bool):
    #     messages = []
    #     fqa = self.file_queue_adapter_factory(f'pref-n{number_of_iterations}-c{commit_messages}')

    #     for i in range(0, number_of_iterations):
    #         m = Message(payload=f'm{i}')
    #         fqa.enqueue(m)
    #         messages.append(m)

    #     for i in range(0, number_of_iterations):
    #         dq_m = fqa.dequeue_next()
    #         self.assertIsNotNone(dq_m)
    #         if commit_messages:
    #             fqa.commit(message=dq_m)

    #         m = messages[i]
    #         self.assertEqual(dq_m.id, m.id)

    #     dq_m = fqa.dequeue_next()
    #     self.assertIsNone(dq_m)

