import os
import time
import subprocess
import unittest
import datetime
from typing import Callable
from greenstalk import (DEFAULT_TUBE, Address, Client)
from statman import Statman
from pulpo_messaging.beanstalkd_queue_adapter import BeanstalkdQueueAdapter
from pulpo_messaging.kessel import QueueAdapter
from pulpo_messaging.kessel import Message

BEANSTALKD_PATH = os.getenv("BEANSTALKD_PATH", "beanstalkd")
DEFAULT_INET_ADDRESS = ("127.0.0.1", 4444)

TestFunc = Callable[[Client], None]
WrapperFunc = Callable[[], None]
DecoratorFunc = Callable[[TestFunc], WrapperFunc]

# pylint: disable=duplicate-code


def with_beanstalkd(address: Address = DEFAULT_INET_ADDRESS, encoding: str = "utf-8", tube: str = DEFAULT_TUBE, reserve_timeout: int = None, max_number_of_attempts: int = None) -> DecoratorFunc:
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
                    options['reserve_timeout'] = reserve_timeout
                    options['max_number_of_attempts'] = max_number_of_attempts

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

    @with_beanstalkd()
    def test_flush(self, qa: QueueAdapter):
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        qa.flush()

        dq_2 = qa.dequeue()
        self.assertIsNone(dq_2)


class TestBeanstalkQueueAdapterDelay(unittest.TestCase):

    @with_beanstalkd(reserve_timeout=None)
    def test_reserve_with_delay_not_specified(self, qa: BeanstalkdQueueAdapter):
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        assert dq_1
        assert dq_1.id == m1.id

    @with_beanstalkd(reserve_timeout=None)
    def test_reserve_with_delay_0(self, qa: BeanstalkdQueueAdapter):
        delay = 0
        m1 = Message(payload='hello world', delay=delay)
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        assert dq_1
        assert dq_1.id == m1.id

    @with_beanstalkd(reserve_timeout=None)
    def test_reserve_with_delay_none(self, qa: BeanstalkdQueueAdapter):
        delay = None
        m1 = Message(payload='hello world', delay=delay)
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        assert dq_1
        assert dq_1.id == m1.id

    @with_beanstalkd(reserve_timeout=None)
    def test_reserve_with_delay(self, qa: BeanstalkdQueueAdapter):
        delay = 2
        m1 = Message(payload='hello world', delay=delay)
        assert m1.delayInSeconds == delay
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        print(f'{dq_1=}')
        # because of delay, the message should not be ready
        assert not dq_1

        time.sleep(delay)

        dq_2 = qa.dequeue()
        print(f'{dq_2=}')
        # because we are now past delay, the message should be ready
        assert dq_2
        assert dq_2.id == m1.id

    @with_beanstalkd(reserve_timeout=None)
    def test_reserve_with_delay_access_another_message(self, qa: BeanstalkdQueueAdapter):
        delay = 1
        m1 = Message(payload='hello world', delay=delay)
        m2 = Message(payload='hello world', delay=0)
        m1 = qa.enqueue(m1)
        m2 = qa.enqueue(m2)

        dq_1 = qa.dequeue()
        assert dq_1.id == m2.id

        dq_2 = qa.dequeue()
        assert not dq_2

        time.sleep(delay)

        dq_3 = qa.dequeue()
        assert dq_3
        assert dq_3.id == m1.id


class TestBeanstalkQueueAdapterReserveTimeoutDuration(unittest.TestCase):

    @with_beanstalkd(reserve_timeout=None)
    def test_reserve_timeout_default(self, qa: BeanstalkdQueueAdapter):
        assert qa.config.reserve_timeout == 0

    @with_beanstalkd(reserve_timeout=None)
    def test_reserve_timeout_none(self, qa: BeanstalkdQueueAdapter):
        Statman.stopwatch('reserve-timer').start()
        qa.dequeue()
        Statman.stopwatch('reserve-timer').stop()
        self.assertAlmostEqual(Statman.stopwatch('reserve-timer').value, 0, delta=0.1)

    @with_beanstalkd(reserve_timeout=2)
    def test_reserve_timeout_two(self, qa: BeanstalkdQueueAdapter):
        Statman.stopwatch('reserve-timer').start()
        qa.dequeue()
        Statman.stopwatch('reserve-timer').stop()
        self.assertAlmostEqual(Statman.stopwatch('reserve-timer').value, 2, delta=0.1)


class TestBeanstalkExpiration(unittest.TestCase):

    @with_beanstalkd(reserve_timeout=None)
    def test_skip_expired_message(self, qa: BeanstalkdQueueAdapter):
        expiration_date_in_past = datetime.datetime.strptime("2000-01-01 12:00:00", "%Y-%m-%d %H:%M:%S")
        print(f'{expiration_date_in_past=}')
        m1 = Message(payload='hello world', expiration=expiration_date_in_past)
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        self.assertIsNone(dq_1)

    @with_beanstalkd(reserve_timeout=None)
    def test_process_message_with_future_expiration(self, qa: BeanstalkdQueueAdapter):
        expiration_date_in_future = datetime.datetime.strptime("3000-01-01 12:00:00", "%Y-%m-%d %H:%M:%S")
        print(f'{expiration_date_in_future=}')
        m1 = Message(payload='hello world', expiration=expiration_date_in_future)
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        self.assertIsNotNone(dq_1)

    @with_beanstalkd(reserve_timeout=None)
    def test_message_with_no_expiration_is_processed(self, qa: BeanstalkdQueueAdapter):
        m1 = Message(payload='hello world', expiration=None)
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        self.assertIsNotNone(dq_1)


class TestBqaMaxAttempts(unittest.TestCase):

    @with_beanstalkd(reserve_timeout=None, max_number_of_attempts=5)
    def test_rollback_increments_attempts(self, qa: BeanstalkdQueueAdapter):
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

    @with_beanstalkd(reserve_timeout=None, max_number_of_attempts=2)
    def test_message_exceeds_attempts_unavailable(self, qa: BeanstalkdQueueAdapter):
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        dq_1 = qa.dequeue()
        self.assertIsNotNone(dq_1)
        self.assertEqual(dq_1.id, m1.id)
        qa.rollback(dq_1)

        dq_2 = qa.dequeue()
        self.assertIsNotNone(dq_2)
        self.assertEqual(dq_2.id, m1.id)
        qa.rollback(dq_2)

        dq_3 = qa.dequeue()
        self.assertIsNone(dq_3)

    @with_beanstalkd(reserve_timeout=None, max_number_of_attempts=2)
    def test_message_exceeds_attempts_process_next_message(self, qa: BeanstalkdQueueAdapter):
        m1 = Message(payload='hello world')
        m1 = qa.enqueue(m1)

        m2 = Message(payload='hello world, again')
        m2 = qa.enqueue(m2)

        dq_1 = qa.dequeue()
        self.assertIsNotNone(dq_1)
        self.assertEqual(dq_1.id, m1.id)
        qa.rollback(dq_1)

        dq_2 = qa.dequeue()
        self.assertIsNotNone(dq_2)
        self.assertEqual(dq_2.id, m1.id)
        qa.rollback(dq_2)

        dq_3 = qa.dequeue()
        self.assertIsNotNone(dq_3)
        self.assertEqual(dq_3.id, m2.id)


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

        dq_1 = qa.dequeue()  # pylint: disable=unused-variable
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


# pylint: enable=duplicate-code
