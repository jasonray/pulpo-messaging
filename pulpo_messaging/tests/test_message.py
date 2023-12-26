import unittest
import datetime
from pulpo_messaging.kessel import Message


class TestMessage(unittest.TestCase):

    def test_message_get_empty(self):
        m = Message()
        print(f'{m}')
        self.assertIsNone(m.get('x'))

    def test_message_set_get(self):
        m = Message()
        print(f'{m}')
        m.set('k1', 'v1')
        m.set('k2', 'v2')
        self.assertEqual(m.get('k1'), 'v1')
        self.assertEqual(m.get('k2'), 'v2')

    def test_message_set_get_nested(self):
        m = Message()
        print(f'{m}')
        m.set('parent.k1', 'v1')
        m.set('parent.k2', 'v2')
        self.assertEqual(m.get('parent.k1'), 'v1')
        self.assertEqual(m.get('parent.k2'), 'v2')

    def test_message_payload(self):
        m = Message(payload='v1')
        self.assertEqual(m.get('body.payload'), 'v1')
        self.assertEqual(m.body.get('payload'), 'v1')
        self.assertEqual(m.get_body_item('payload'), 'v1')
        self.assertEqual(m.payload, 'v1')

    def test_message_payload_not_get(self):
        m = Message()
        self.assertIsNone(m.payload)

    def test_message_body_constructor(self):
        b = {'k1': 'v1', 'k2': 'v2'}
        m = Message(body=b)

        print('test_message_body_constructor checks')
        body = m.body
        print(f'{body=}')
        print(f"{body.get('k1')=}")

        self.assertEqual(m.body.get('k1'), 'v1')
        self.assertEqual(m.body.get('k2'), 'v2')

    def test_message_body_set(self):
        m = Message()
        m.set_body_item('k1', 'v1')
        m.set_body_item('k2', 'v2')
        self.assertEqual(m.body.get('k1'), 'v1')
        self.assertEqual(m.body.get('k2'), 'v2')

    def test_message_request_type(self):
        m = Message(request_type='rt')
        print(f'{m}')
        self.assertEqual(m.request_type, 'rt')
        self.assertEqual(m.get_header_item('request_type'), 'rt')

    def test_message_id(self):
        m = Message(message_id=123)
        print(f'{m}')
        self.assertEqual(m.id, '123')

    def test_message_expiration(self):
        dt = datetime.datetime.now
        m = Message(expiration=dt)
        print(f'{m}')
        self.assertEqual(m.expiration, dt)

    def test_message_header_constructor(self):
        h = {'k1': 'v1', 'k2': 'v2', 'h1': None}
        m = Message(header=h)

        self.assertEqual(m.header.get('k1'), 'v1')
        self.assertEqual(m.header.get('k2'), 'v2')
        self.assertEqual(m.header.get('h1'), None)

    def test_message_header_set(self):
        m = Message()
        m.set_header_item('k1', 'v1')
        m.set_header_item('k2', 'v2')
        m.set_header_item('h1')
        self.assertEqual(m.header.get('k1'), 'v1')
        self.assertEqual(m.header.get('k2'), 'v2')
        self.assertEqual(m.header.get('h1'), None)

    def test_message_attempts(self):
        m = Message()
        m.attempts = 3
        print(f'{m}')
        self.assertEqual(m.attempts, 3)

        m.attempts = 5
        print(f'{m}')
        self.assertEqual(m.attempts, 5)


class TestMessageDelta(unittest.TestCase):

    def test_message_delay_as_date(self):
        dt = datetime.datetime.now()
        m = Message(delay=dt)
        print(f'{m}')
        self.assertEqual(m.delay, dt)

    def test_message_delay_as_delta(self):
        delta = datetime.timedelta(seconds=20)
        dt = datetime.datetime.now() + delta
        m = Message(delay=delta)
        print(f'{m}')
        # dates are almost equal
        assert abs(m.delay - dt).seconds < 1

    def test_message_delay_as_int(self):
        delta_in_seconds = 20
        delta = datetime.timedelta(seconds=delta_in_seconds)
        dt = datetime.datetime.now() + delta
        m = Message(delay=delta_in_seconds)
        print(f'{m}')
        # dates are almost equal
        assert abs(m.delay - dt).seconds < 1

    def test_message_delay_as_int_get_in_seconds(self):
        delta_in_seconds = 20
        m = Message(delay=delta_in_seconds)
        print(f'{m}')
        # dates are almost equal
        print(f'm delay: {m.delay=}')
        assert (m.delayInSeconds - delta_in_seconds) < 1
