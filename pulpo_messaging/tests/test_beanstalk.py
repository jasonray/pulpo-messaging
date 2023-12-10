import os
import time
import subprocess
import unittest
from pulpo_messaging.beanstalkd_queue_adapter import BeanstalkdQueueAdapter
from pulpo_messaging.kessel import QueueAdapter
from pulpo_messaging.kessel import Message
from typing import Callable, Optional

from greenstalk import (DEFAULT_TUBE, Address, Client)

BEANSTALKD_PATH = os.getenv("BEANSTALKD_PATH", "beanstalkd")
DEFAULT_INET_ADDRESS = ("127.0.0.1", 4444)

TestFunc = Callable[[Client], None]
WrapperFunc = Callable[[], None]
DecoratorFunc = Callable[[TestFunc], WrapperFunc]


def with_beanstalkd(
    address: Address = DEFAULT_INET_ADDRESS,
    encoding: str = "utf-8",
    tube: str = DEFAULT_TUBE,
) -> DecoratorFunc:
    print('with_beanstalkd')
    print('define decorator')

    def decorator(test: TestFunc) -> WrapperFunc:

        def wrapper(cls) -> None:
            cmd = [BEANSTALKD_PATH]
            host, port = address
            cmd.extend(["-l", host, "-p", str(port)])
            print(f'starting beanstalkd [{cmd=}][{address=}]')
            with subprocess.Popen(cmd) as beanstalkd:
                print(f'started beanstalkd {beanstalkd.pid=}')
                time.sleep(0.1)
                try:
                    options = {}
                    options['host'] = host
                    options['port'] = port
                    options['encoding'] = encoding
                    options['default_tube'] = tube
                    bqa = BeanstalkdQueueAdapter(options=options)
                    test(cls, bqa)
                finally:
                    print(f'terminating beanstalkd {beanstalkd.pid=}')
                    beanstalkd.terminate()

        return wrapper

    return decorator


class TestBeanstalkQueueAdapterCompliance(unittest.TestCase):

    @with_beanstalkd()
    def test_enqueue_dequeue_message(self, qa: QueueAdapter):
        print(f'test_enqueue_dequeue_message {qa=}')

        m1 = Message(payload='hello world')
        print('invoke enqueue')
        m1 = qa.enqueue(m1)
        print(f'enqueue complete {m1.id=}')

        print('invoke dequeue')
        dq_1 = qa.dequeue()
        print(f'dequeue complete {dq_1=}')

        # I am not sure which assertion style I should use, so leaving both here for now
        assert dq_1.id == m1.id
        self.assertEqual(dq_1.id, m1.id)
        assert dq_1.payload == 'hello world'

    @with_beanstalkd()
    def test_enqueue_dequeue_with_body(self, qa: QueueAdapter):
        m1 = Message(body={'k': 'v'}, payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()

        assert dq_1.payload == 'hello world'
        assert dq_1.get_body_item('k') == 'v'
        assert dq_1.id == m1.id

    @with_beanstalkd()
    def test_enqueue_dequeue_with_header(self, qa: QueueAdapter):
        m1 = Message(header={'k': 'v'})
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()

        assert dq_1.get_header_item('k') == 'v'
        assert dq_1.id == m1.id

    @with_beanstalkd()
    def test_dequeue_skip_locked_message_with_1(self, qa: QueueAdapter):
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        assert dq_1

        dq_2 = qa.dequeue()
        assert not dq_2

    @with_beanstalkd()
    def test_dequeue_skip_locked_message_with_2(self, qa: QueueAdapter):
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
        assert dq1.payload == m1.payload
        print('second dequeue, expect m2')
        dq2 = qa.dequeue()
        assert dq2.payload == m2.payload

        # because message is locked, should not be able to dequeue
        dq3 = qa.dequeue()
        assert not dq3

    @with_beanstalkd()
    def test_commit_removes_message(self, qa: QueueAdapter):
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        assert dq_1

        # at this point, m1 should exist and be locked

        # because message is locked, it should not be available for dequeue
        dq_2 = qa.dequeue()
        assert not dq_2

        qa.commit(dq_1)

        # at this point, m1 should not exist

        dq_3 = qa.dequeue()
        assert not dq_3

    @with_beanstalkd()
    def test_rollback_removes_lock(self, qa: QueueAdapter):
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        assert dq_1

        # at this point, m1 should exist and be locked

        # because message is locked, it should not be available for dequeue
        dq_2 = qa.dequeue()
        assert not dq_2

        qa.rollback(dq_1)

        # at this point, m1 should exist and be available for dequeue

        dq_3 = qa.dequeue()
        assert dq_3


class TestBeanstalkQueueAdapterStats():

    @with_beanstalkd()
    def test_stats_not_null(self, qa: BeanstalkdQueueAdapter):
        stats = qa.beanstalk_stat()
        print(f'{stats=}')
        assert stats

    @with_beanstalkd()
    def test_enqueue_one_item(self, qa: BeanstalkdQueueAdapter):
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        stats = qa.beanstalk_stat()
        print(f'{stats=}')
        assert stats['current-jobs-reserved'] == 0
        assert stats['current-jobs-ready'] == 1
        assert stats['total-jobs'] == 1

    @with_beanstalkd()
    def test_enqueue_two_dequeue_one_item(self, qa: BeanstalkdQueueAdapter):
        m1 = Message(payload='hello world 1')
        m1 = qa.enqueue(m1)
        m2 = Message(payload='hello world 2')
        m2 = qa.enqueue(m2)

        dq_1 = qa.dequeue()  # pylint: disable=unused-variable

        stats = qa.beanstalk_stat()
        print(f'{stats=}')
        assert stats['current-jobs-reserved'] == 1
        assert stats['current-jobs-ready'] == 1
        assert stats['total-jobs'] == 2

    @with_beanstalkd()
    def test_enqueue_four_dequeue_two_item_commit_one(self, qa: BeanstalkdQueueAdapter):
        m1 = Message(payload='hello world 1')
        m1 = qa.enqueue(m1)
        m2 = Message(payload='hello world 2')
        m2 = qa.enqueue(m2)
        m3 = Message(payload='hello world 3')
        m3 = qa.enqueue(m3)
        m4 = Message(payload='hello world 4')
        m4 = qa.enqueue(m4)

        dq_1 = qa.dequeue()
        dq_2 = qa.dequeue()

        qa.commit(dq_2)

        stats = qa.beanstalk_stat()
        print(f'{stats=}')
        assert stats['current-jobs-reserved'] == 1
        assert stats['current-jobs-ready'] == 2
        assert stats['total-jobs'] == 4

    @with_beanstalkd()
    def test_enqueue_four_dequeue_two_item_commit_one_release_one(self, qa: BeanstalkdQueueAdapter):
        m1 = Message(payload='hello world 1')
        m1 = qa.enqueue(m1)
        m2 = Message(payload='hello world 2')
        m2 = qa.enqueue(m2)
        m3 = Message(payload='hello world 3')
        m3 = qa.enqueue(m3)
        m4 = Message(payload='hello world 4')
        m4 = qa.enqueue(m4)

        dq_1 = qa.dequeue()
        dq_2 = qa.dequeue()

        qa.commit(dq_2)
        qa.rollback(dq_1)

        stats = qa.beanstalk_stat()
        print(f'{stats=}')
        assert stats['current-jobs-reserved'] == 0
        assert stats['current-jobs-ready'] == 3
        assert stats['cmd-delete'] == 1
        assert stats['total-jobs'] == 4
