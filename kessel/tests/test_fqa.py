import datetime
import uuid
import os
import unittest
from kessel.kessel import FileQueueAdapter
from kessel.kessel import Message
import time


class TestFqa(unittest.TestCase):
    _kessel_root_directory = '/tmp/kessel/unit-test'
    _kessel_directory = os.path.join(_kessel_root_directory, datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

    def test_construct(self):
        fqa = FileQueueAdapter(base_path=self.get_unique_base_path())
        self.assertIsNotNone(fqa)

    def test_publish_message(self):
        fqa = FileQueueAdapter(base_path=self.get_unique_base_path())
        m = Message(payload='hello world')
        m = fqa.enqueue(m)
        self.assertIsNotNone(m)

    def test_dequeue_message(self):
        fqa = FileQueueAdapter(base_path=self.get_unique_base_path())
        m1 = Message(payload='hello world')
        m1_path_file = fqa.enqueue(m1)

        m2 = fqa.dequeue_next()
        self.assertEqual(m2.header, 'm1.0')
        self.assertEqual(m2.payload, 'hello world')

        # check that lock file exists
        expected_lock_file = fqa.get_lock_path(m2.id)
        print('checking lock file', expected_lock_file)
        self.assertTrue(os.path.exists(expected_lock_file))

    def test_two_cannot_dequeue_same_record(self):
        fqa = FileQueueAdapter(base_path=self.get_unique_base_path())
        m1 = Message(payload='hello world')
        m1_path_file = fqa.enqueue(m1)

        m2 = fqa.dequeue_next()

        # because message is locked, should not be able to dequeue
        m3 = fqa.dequeue_next()
        self.assertIsNone(m3)

    def test_two_dequeue_nonlocked_record(self):
        fqa = FileQueueAdapter(base_path=self.get_unique_base_path())
        m1 = Message(payload='hello world m1')
        print('enqueue m1')
        m1_path_file = fqa.enqueue(m1)
        print('enqueue complete', m1_path_file)

        m2 = Message(payload='hello world m2')
        print('enqueue m2')
        m2_path_file = fqa.enqueue(m2)
        print('enqueue complete', m2_path_file)

        print('first dequeue, expect m1')
        dq1 = fqa.dequeue_next()
        self.assertEqual(dq1.payload, m1.payload)
        print('first dequeue, expect m2')
        dq2 = fqa.dequeue_next()
        self.assertEqual(dq2.payload, m2.payload)

        # because message is locked, should not be able to dequeue
        dq3 = fqa.dequeue_next()
        self.assertIsNone(dq3)

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
