import os
import unittest
import time
import datetime
import json
from datetime import timedelta
from pulpo_messaging.beanstalkd_queue_adapter import BeanstalkdQueueAdapter
from pulpo_messaging.kessel import FileQueueAdapter
from pulpo_messaging.kessel import QueueAdapter
from pulpo_messaging.kessel import Message
from .unittest_helper import get_unique_base_path

import json
import os
import signal
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Callable, Iterable, Iterator, Optional, Union

# BEANSTALKD_PATH = os.getenv("BEANSTALKD_PATH", "beanstalkd")
# DEFAULT_INET_ADDRESS = ("127.0.0.1", 4444)
# DEFAULT_UNIX_ADDRESS = "/tmp/greenstalk-test.sock"

# TestFunc = Callable[[Client], None]
# WrapperFunc = Callable[[], None]
# DecoratorFunc = Callable[[TestFunc], WrapperFunc]


# def with_beanstalkd(
#     address: Address = DEFAULT_INET_ADDRESS,
#     encoding: Optional[str] = "utf-8",
#     use: str = DEFAULT_TUBE,
#     watch: Union[str, Iterable[str]] = DEFAULT_TUBE,
# ) -> DecoratorFunc:
#     def decorator(test: TestFunc) -> WrapperFunc:
#         def wrapper() -> None:
#             cmd = [BEANSTALKD_PATH]
#             if isinstance(address, str):
#                 cmd.extend(["-l", "unix:" + address])
#             else:
#                 host, port = address
#                 cmd.extend(["-l", host, "-p", str(port)])
#             with subprocess.Popen(cmd) as beanstalkd:
#                 time.sleep(0.1)
#                 try:
#                     with Client(address, encoding=encoding, use=use, watch=watch) as c:
#                         test(c)
#                 finally:
#                     beanstalkd.terminate()

#         return wrapper

#     return decorator


class TestBeanstalkQueueAdapterCompliance(unittest.TestCase):
    # To start beanstalkd now and restart at login:
    #   brew services start beanstalkd
    # Or, if you don't want/need a background service you can just run:
    #   /opt/homebrew/opt/beanstalkd/bin/beanstalkd -l 127.0.0.1 -p 11300

    def queue_adapter_factory(self) -> QueueAdapter:
        options = {}
        options['host'] = '127.0.0.1'
        options['port'] = 11300
        options['socket_timeout'] = 10
        return BeanstalkdQueueAdapter(options=options)

    def test_enqueue_dequeue_message(self):
        qa = self.queue_adapter_factory()
        m1 = Message(payload='hello world')
        print('invoke enqueue')
        m1 = qa.enqueue(m1)
        print(f'enqueue complete {m1.id=}')

        print('invoke dequeue')
        dq_1 = qa.dequeue()
        print(f'dequeue complete {dq_1=}')

        self.assertEqual(dq_1.id, m1.id)
        self.assertEqual(dq_1.payload, 'hello world')

    # def test_enqueue_dequeue_with_body(self):
    #     qa = self.queue_adapter_factory()
    #     m1 = Message(body={'k': 'v'}, payload='hello world')
    #     m1 = qa.enqueue(m1)

    #     dq_1 = qa.dequeue()

    #     self.assertEqual(dq_1.payload, 'hello world')
    #     self.assertEqual(dq_1.get_body_item('k'), 'v')
    #     self.assertEqual(dq_1.id, m1.id)

    # def test_enqueue_dequeue_with_header(self):
    #     qa = self.queue_adapter_factory()
    #     m1 = Message(header={'k': 'v'})
    #     m1 = qa.enqueue(m1)

    #     dq_1 = qa.dequeue()

    #     self.assertEqual(dq_1.get_header_item('k'), 'v')
    #     self.assertEqual(dq_1.id, m1.id)

    # def test_dequeue_skip_locked_message_with_1(self):
    #     qa = self.queue_adapter_factory()
    #     m1 = Message(payload='hello world')
    #     m1 = qa.enqueue(m1)

    #     dq_1 = qa.dequeue()
    #     self.assertIsNotNone(dq_1)

    #     dq_2 = qa.dequeue()
    #     self.assertIsNone(dq_2)

    # def test_dequeue_skip_locked_message_with_2(self):
    #     qa = self.queue_adapter_factory()
    #     m1 = Message(payload='hello world m1')
    #     print('enqueue m1')
    #     m1 = qa.enqueue(m1)
    #     print('enqueue complete', m1.id)

    #     m2 = Message(payload='hello world m2')
    #     print('enqueue m2')
    #     m2 = qa.enqueue(m2)
    #     print('enqueue complete', m2.id)

    #     print('first dequeue, expect m1')
    #     dq1 = qa.dequeue()
    #     self.assertEqual(dq1.payload, m1.payload)
    #     print('second dequeue, expect m2')
    #     dq2 = qa.dequeue()
    #     self.assertEqual(dq2.payload, m2.payload)

    #     # because message is locked, should not be able to dequeue
    #     dq3 = qa.dequeue()
    #     self.assertIsNone(dq3)

    # def test_commit_removes_message(self):
    #     qa = self.queue_adapter_factory()
    #     m1 = Message(payload='hello world')
    #     m1 = qa.enqueue(m1)

    #     dq_1 = qa.dequeue()
    #     self.assertIsNotNone(dq_1)

    #     # at this point, m1 should exist and be locked

    #     # because message is locked, it should not be available for dequeue
    #     dq_2 = qa.dequeue()
    #     self.assertIsNone(dq_2)

    #     qa.commit(dq_1)

    #     # at this point, m1 should not exist

    #     dq_3 = qa.dequeue()
    #     self.assertIsNone(dq_3)

    # def test_rollback_removes_lock(self):
    #     qa = self.queue_adapter_factory()
    #     m1 = Message(payload='hello world')
    #     m1 = qa.enqueue(m1)

    #     dq_1 = qa.dequeue()
    #     self.assertIsNotNone(dq_1)

    #     # at this point, m1 should exist and be locked

    #     # because message is locked, it should not be available for dequeue
    #     dq_2 = qa.dequeue()
    #     self.assertIsNone(dq_2)

    #     qa.rollback(dq_1)

    #     # at this point, m1 should exist and be available for dequeue

    #     dq_3 = qa.dequeue()
    #     self.assertIsNotNone(dq_3)
