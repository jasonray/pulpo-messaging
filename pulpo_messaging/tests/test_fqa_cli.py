import os
import re
import subprocess
import unittest
from .unittest_helper import get_unique_base_path


class TestFqaCli(unittest.TestCase):
    fqa_config = 'pulpo-config-fqa.json'

    def run_cli(self, command, config: str = None, fqa_base_directory: str = None, additional_args=None, error_on_fail: bool = True):
        if not config:
            config = self.fqa_config

        if not fqa_base_directory:
            fqa_base_directory = get_unique_base_path()

        args = []
        args.append('python3')
        args.append('pulpo-cli.py')
        args.append(command)
        args.append(f'--config={config}')
        args.append(f'--file_queue_adapter.base_path={fqa_base_directory}')

        if additional_args:
            args = args + additional_args

        print(f'{args=}')
        result = subprocess.run(args, capture_output=True, check=error_on_fail)
        print(f'{command=} => {result=}')
        return result

    def run_put(self, message: str = None, fqa_base_directory: str = None, error_on_fail: bool = True):
        if not message:
            message = 'hello world'

        additional_args = [f'--payload="{message}"']
        result = self.run_cli(command='queue.put', config=self.fqa_config, fqa_base_directory=fqa_base_directory, additional_args=additional_args, error_on_fail=error_on_fail)

        if result.returncode == 0:
            message_id = self.get_message_id_from_output(result)
            print(f'run put cli [{message_id=}]')
        else:
            message_id = None

        return result, message_id

    def run_pop(self, fqa_base_directory: str = None, error_on_fail: bool = True):
        result = self.run_cli(command='queue.pop', config=self.fqa_config, fqa_base_directory=fqa_base_directory, error_on_fail=error_on_fail)

        message_id = self.get_message_id_from_output(result)
        print(f'run pop cli [{message_id=}]')
        return result, message_id

    def run_peek(self, message_id: str, fqa_base_directory: str = None, error_on_fail: bool = True):
        additional_args = [f'--id={message_id}']
        result = self.run_cli(command='queue.peek', config=self.fqa_config, fqa_base_directory=fqa_base_directory, additional_args=additional_args, error_on_fail=error_on_fail)
        print('run peek cli')
        return result

    def run_delete(self, message_id: str, fqa_base_directory: str = None, error_on_fail: bool = True):
        additional_args = [f'--id={message_id}']
        result = self.run_cli(command='queue.delete', config=self.fqa_config, fqa_base_directory=fqa_base_directory, additional_args=additional_args, error_on_fail=error_on_fail)
        print('run delete cli')
        return result

    def get_message_id_from_output(self, result):
        # print(f'{result=}')
        output = result.stdout
        output_str = output.decode('utf-8')
        # print(f'attempt to extract message id from string: {output_str}')
        match = re.search(r"(?:message\.id|message_id)='([^']+)'", output_str)

        if match:
            message_id = match.group(1)
        else:
            message_id = None

        return message_id

    def test_put(self):
        _, message_id = self.run_put(message='hello world!')
        self.assertIsNotNone(message_id)

    def test_fail_put(self):
        # create a directory which the application has no permissions to write
        # this will cause failure
        fqa_base_directory = os.path.join(get_unique_base_path("cli"), 'invalid')
        os.makedirs(fqa_base_directory)
        os.chmod(path=fqa_base_directory, mode=0)

        with self.assertRaises(Exception):
            self.run_put(message='this will fail', fqa_base_directory=fqa_base_directory)

    def test_put_peek(self):
        fqa_base_directory = get_unique_base_path("cli")
        result, message_id = self.run_put(message='hello world', fqa_base_directory=fqa_base_directory)

        result = self.run_peek(message_id=message_id, fqa_base_directory=fqa_base_directory, error_on_fail=True)
        assert result.returncode == 0
        assert 'hello world' in str(result.stdout)

    def test_put_pop_peek(self):
        fqa_base_directory = get_unique_base_path("cli")

        result, put_message_id = self.run_put(message='hello world', fqa_base_directory=fqa_base_directory, error_on_fail=True)
        self.assertIsNotNone(put_message_id)

        result, pop_message_id = self.run_pop(fqa_base_directory=fqa_base_directory, error_on_fail=True)
        self.assertIsNotNone(pop_message_id)
        self.assertEqual(pop_message_id, put_message_id)

        result = self.run_peek(put_message_id, fqa_base_directory=fqa_base_directory)
        assert 'no message' in str(result.stdout)

    def test_put_delete_peek(self):
        fqa_base_directory = get_unique_base_path("cli")

        result, put_message_id = self.run_put(message='hello world', fqa_base_directory=fqa_base_directory)
        self.assertIsNotNone(put_message_id)

        result = self.run_delete(message_id=put_message_id, fqa_base_directory=fqa_base_directory)

        result = self.run_peek(put_message_id, fqa_base_directory=fqa_base_directory)
        assert 'no message' in str(result.stdout)

    def test_put_empty(self):
        fqa_base_directory = get_unique_base_path("cli")

        result, pop_message_id = self.run_pop(fqa_base_directory=fqa_base_directory, error_on_fail=True)
        self.assertIsNone(pop_message_id)
        assert 'no message' in str(result.stdout)
