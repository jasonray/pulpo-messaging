import datetime
import uuid
import os
import unittest
from kessel.kessel import FileQueueAdapter
from kessel.kessel import Message
import time


class TestFqa(unittest.TestCase):
    _kessel_root_directory = '/tmp/kessel/unit-test'
    _kessel_directory = os.path.join(
        _kessel_root_directory,
        datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

    def test_construct(self):
        fqa = FileQueueAdapter(base_path=self.get_unique_base_path())
        self.assertIsNotNone(fqa)

    def test_publish_message(self):
        fqa = FileQueueAdapter(base_path=self.get_unique_base_path())
        m = Message(payload='hello world')
        m = fqa.enqueue(m)

        self.assertTrue(fqa._does_message_exist(m.id))

    def test_dequeue_message(self):
        fqa = FileQueueAdapter(base_path=self.get_unique_base_path())
        m1 = Message(payload='hello world')
        m1 = fqa.enqueue(m1)

        dq_1 = fqa.dequeue_next()
        self.assertEqual(dq_1.payload, 'hello world')
        self.assertEqual(dq_1.id, m1.id)

        # messsage should exist, lock should exist
        self.assertTrue(fqa._does_message_exist(m1.id))
        self.assertTrue(fqa._does_lock_exist(m1.id))

    def test_two_cannot_dequeue_same_record(self):
        fqa = FileQueueAdapter(base_path=self.get_unique_base_path())
        m1 = Message(payload='hello world')
        m1 = fqa.enqueue(m1)

        dq_1 = fqa.dequeue_next()
        self.assertIsNotNone(dq_1)

        # because message is locked, should not be able to dequeue
        dq_2 = fqa.dequeue_next()
        self.assertIsNone(dq_2)

    def test_two_dequeue_nonlocked_record(self):
        fqa = FileQueueAdapter(base_path=self.get_unique_base_path())
        m1 = Message(payload='hello world m1')
        print('enqueue m1')
        m1 = fqa.enqueue(m1)
        print('enqueue complete', m1.id)

        m2 = Message(payload='hello world m2')
        print('enqueue m2')
        m2_path_file = fqa.enqueue(m2)
        print('enqueue complete', m2.id)

        print('first dequeue, expect m1')
        dq1 = fqa.dequeue_next()
        self.assertEqual(dq1.payload, m1.payload)
        print('first dequeue, expect m2')
        dq2 = fqa.dequeue_next()
        self.assertEqual(dq2.payload, m2.payload)

        # because message is locked, should not be able to dequeue
        dq3 = fqa.dequeue_next()
        self.assertIsNone(dq3)

    def test_commit_removes_message(self):
        fqa = FileQueueAdapter(base_path=self.get_unique_base_path())
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
        fqa = FileQueueAdapter(base_path=self.get_unique_base_path())
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

    def get_unique_base_path(self, tag: str = None):
        path = self._kessel_directory
        print('get_unique_base_path p1', path)
        if tag:
            path = os.path.join(path, tag)
        print('get_unique_base_path p2', path)
        path = os.path.join(path, str(uuid.uuid4()))
        print('get_unique_base_path p3', path)
        return path

    def test_enqueue_dequeue_x100(self):
        message_count = 100
        messages=[]
        fqa = FileQueueAdapter(base_path=self.get_unique_base_path())

        for i in range(0,message_count):
            m=Message(payload=f'm{i}') 
            fqa.enqueue(m)
            messages.append(m)

        for i in range(0,message_count):
            dq_m = fqa.dequeue_next()
            self.assertIsNotNone(dq_m)
            # fqa.commit(message=dq_m)

            m=messages[i]
            self.assertEqual(dq_m.id, m.id)

        dq_m = fqa.dequeue_next()
        self.assertIsNone(dq_m)