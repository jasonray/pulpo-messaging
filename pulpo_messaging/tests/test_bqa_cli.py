import os
import subprocess
import time
from typing import Callable
import unittest
from .unittest_helper import get_message_id_from_output, get_unique_base_path

BEANSTALKD_PATH = os.getenv("BEANSTALKD_PATH", "beanstalkd")
DEFAULT_INET_ADDRESS = ("127.0.0.1", 4444)

TestFunc = Callable[[], None]
WrapperFunc = Callable[[], None]
DecoratorFunc = Callable[[TestFunc], WrapperFunc]


def with_beanstalkd() -> DecoratorFunc:

    def decorator(test: TestFunc) -> WrapperFunc:

        def wrapper(cls) -> None:
            cmd = [BEANSTALKD_PATH]
            host = '127.0.0.1'
            port = 11300
            address = (host, port)
            cmd.extend(["-l", host, "-p", str(port)])
            print(f'starting beanstalkd [{cmd=}][{address=}]')
            with subprocess.Popen(cmd) as beanstalkd:
                print(f'started beanstalkd {beanstalkd.pid=}')
                time.sleep(0.1)
                try:
                    test(cls)
                finally:
                    print(f'terminating beanstalkd {beanstalkd.pid=}')
                    beanstalkd.terminate()

        return wrapper

    return decorator


class TestBqaCli(unittest.TestCase):
    bqa_config = 'pulpo-config-beanstalk.json'

    def run_cli(self, command, config: str = None, additional_args=None, error_on_fail: bool = True):
        if not config:
            config = self.bqa_config

        args = []
        args.append('python3')
        args.append('pulpo-cli.py')
        args.append(command)
        args.append(f'--config={config}')

        if additional_args:
            args = args + additional_args

        print(f'{args=}')
        result = subprocess.run(args, capture_output=True, check=error_on_fail)
        print(f'{command=} => {result=}')
        return result

    def run_put(self, payload: str = None, error_on_fail: bool = True):
        if not payload:
            payload = 'hello world'

        additional_args = [f'--payload="{payload}"']
        result = self.run_cli(command='queue.put', config=self.bqa_config, additional_args=additional_args, error_on_fail=error_on_fail)

        if result.returncode == 0:
            message_id = get_message_id_from_output(result)
            print(f'run put cli [{message_id=}]')
        else:
            message_id = None

        return result, message_id

    def run_pop(self, error_on_fail: bool = True):
        result = self.run_cli(command='queue.pop', config=self.bqa_config, error_on_fail=error_on_fail)

        message_id = get_message_id_from_output(result)
        print(f'run pop cli [{message_id=}]')
        return result, message_id

    def run_peek(self, message_id: str, error_on_fail: bool = True):
        additional_args = [f'--id={message_id}']
        result = self.run_cli(command='queue.peek', config=self.bqa_config, additional_args=additional_args, error_on_fail=error_on_fail)
        print('run peek cli')
        return result

    def run_delete(self, message_id: str, error_on_fail: bool = True):
        additional_args = [f'--id={message_id}']
        result = self.run_cli(command='queue.delete', config=self.bqa_config, additional_args=additional_args, error_on_fail=error_on_fail)
        print('run delete cli')
        return result

    @with_beanstalkd()
    def test_put(self):
        _, message_id = self.run_put(payload='hello world!')
        self.assertIsNotNone(message_id)

    # @with_beanstalkd()
    def test_fail_put(self):
        # create a directory which the application has no permissions to write
        # this will cause failure
        fqa_base_directory = os.path.join(get_unique_base_path("cli"), 'invalid')
        os.makedirs(fqa_base_directory)
        os.chmod(path=fqa_base_directory, mode=0)

        with self.assertRaises(Exception):
            self.run_put(payload='this will fail')

    @with_beanstalkd()
    def test_put_peek(self):
        result, message_id = self.run_put(payload='hello world')

        result = self.run_peek(message_id=message_id, error_on_fail=True)
        assert result.returncode == 0
        assert 'hello world' in str(result.stdout)

    @with_beanstalkd()
    def test_put_pop_peek(self):
        result, put_message_id = self.run_put(payload='hello world', error_on_fail=True)  # pylint: disable=unused-variable
        self.assertIsNotNone(put_message_id)

        result, pop_message_id = self.run_pop(error_on_fail=True)
        self.assertIsNotNone(pop_message_id)
        self.assertEqual(pop_message_id, put_message_id)

        with self.assertRaises(Exception):
            self.run_peek(put_message_id)

    @with_beanstalkd()
    def test_put_delete_peek(self):
        _, put_message_id = self.run_put(payload='hello world')
        self.assertIsNotNone(put_message_id)

        self.run_delete(message_id=put_message_id)

        with self.assertRaises(Exception):
            self.run_peek(put_message_id)

    @with_beanstalkd()
    def test_pop_empty(self):
        result, pop_message_id = self.run_pop(error_on_fail=True)
        self.assertIsNone(pop_message_id)
        assert 'no message' in str(result.stdout)
