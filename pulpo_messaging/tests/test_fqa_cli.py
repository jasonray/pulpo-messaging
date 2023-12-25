import os
import re
import subprocess
from typing import Callable
import unittest
import time
import datetime
from datetime import timedelta
from uuid import uuid4
from .unittest_helper import get_unique_base_path


class TestFqaCli(unittest.TestCase):
    fqa_config = 'pulpo-config-fqa.json'

    def run_cli(self, command, config: str, fqa_base_directory: str = None, additional_args=[]):
        args = []
        args.append('python3')
        args.append('pulpo-cli.py')
        args.append(command)
        args.append(f'--config={config}')

        if fqa_base_directory:
            args.append(f'--file_queue_adapter.base_path={fqa_base_directory}')

        args = args + additional_args
        print(f'{args=}')
        result = subprocess.run(args, capture_output=True)
        print(f'{command=} => {result=}')
        return result

    def get_message_id_from_output(self, result):
        print(f'{result=}')
        output = result.stdout
        output_str = output.decode('utf-8')
        print(f'{output_str=}')
        match = re.search(r"message\.id='([^']+)'", output_str)

        self.assertIsNotNone(match, f'unable to determine job id from output [{output_str}]')
        message_id = match.group(1)
        return message_id

    def test_ls(self):
        result = subprocess.run(["ls", "-l", "/dev/null"], capture_output=True)
        print(f'{result=}')
        assert result.returncode == 0
        assert 'root' in str(result.stdout)

    def test_put(self):
        additional_args = ['--payload="hello world!"']
        result = self.run_cli(command='queue.put', config=self.fqa_config, additional_args=additional_args)
        assert result.returncode == 0

        message_id = self.get_message_id_from_output(result)
        print(f'{message_id=}')
        self.assertIsNotNone(message_id)

    def test_put_peek(self):
        additional_args = ['--payload="hello world!"']
        result = self.run_cli(command='queue.put', config=self.fqa_config, additional_args=additional_args)
        assert result.returncode == 0

        message_id = self.get_message_id_from_output(result)
        print(f'{message_id=}')
        self.assertIsNotNone(message_id)

        additional_args = [f'--id={message_id}']
        result = self.run_cli(command='queue.peek', config=self.fqa_config, additional_args=additional_args)
        assert result.returncode == 0
        assert 'hello world' in str(result.stdout)

    def test_put_pop_peek(self):
        fqa_base_directory = get_unique_base_path("cli")

        additional_args = ['--payload="hello world!"']
        result = self.run_cli(command='queue.put', config=self.fqa_config, fqa_base_directory=fqa_base_directory, additional_args=additional_args)
        assert result.returncode == 0

        put_message_id = self.get_message_id_from_output(result)
        print(f'put {put_message_id=}')
        self.assertIsNotNone(put_message_id)

        additional_args = [f'--id={put_message_id}']
        result = self.run_cli(command='queue.pop', config=self.fqa_config, fqa_base_directory=fqa_base_directory, additional_args=additional_args)
        assert result.returncode == 0

        pop_message_id = self.get_message_id_from_output(result)
        print(f'pop {pop_message_id=}')
        self.assertIsNotNone(pop_message_id)
        self.assertEqual(pop_message_id, put_message_id)

        additional_args = [f'--id={put_message_id}']
        result = self.run_cli(command='queue.peek', config=self.fqa_config, fqa_base_directory=fqa_base_directory, additional_args=additional_args)
        assert result.returncode == 0
        assert 'no message' in str(result.stdout)

    def test_put_delete_peek(self):
        fqa_base_directory = get_unique_base_path("cli")

        additional_args = ['--payload="hello world!"']
        result = self.run_cli(command='queue.put', config=self.fqa_config, fqa_base_directory=fqa_base_directory, additional_args=additional_args)
        assert result.returncode == 0

        put_message_id = self.get_message_id_from_output(result)
        print(f'put {put_message_id=}')
        self.assertIsNotNone(put_message_id)

        additional_args = [f'--id={put_message_id}']
        result = self.run_cli(command='queue.delete', config=self.fqa_config, fqa_base_directory=fqa_base_directory, additional_args=additional_args)
        assert result.returncode == 0

        delete_message_id = self.get_message_id_from_output(result)
        print(f'delete {delete_message_id=}')
        self.assertIsNotNone(delete_message_id)
        self.assertEqual(delete_message_id, put_message_id)

        additional_args = [f'--id={put_message_id}']
        result = self.run_cli(command='queue.peek', config=self.fqa_config, fqa_base_directory=fqa_base_directory, additional_args=additional_args)
        assert result.returncode == 0
        assert 'no message' in str(result.stdout)

    # @with_beanstalkd()
    # def test_put_delete_peek(self):
    #     tubename = self.get_tubename()
    #     additional_args = ['--body=sample']
    #     result =self.run_cli(command='put', tubename=tubename , additional_args=additional_args  )
    #     assert result.returncode == 0

    #     job_id = self.get_job_id_from_output(result)
    #     print(f'{job_id=}')
    #     self.assertIsNotNone(job_id)

    #     additional_args = [f'--id={job_id}']
    #     result =self.run_cli(command='peek', tubename=tubename , additional_args=additional_args  )
    #     assert result.returncode == 0
    #     assert 'sample' in str(result.stdout)

    #     print(f'pop {job_id=}')
    #     additional_args = [f'--id={job_id}']
    #     result =self.run_cli(command='delete', tubename=tubename , additional_args=additional_args  )
    #     assert result.returncode == 0

    #     additional_args = [f'--id={job_id}']
    #     result =self.run_cli(command='peek', tubename=tubename , additional_args=additional_args  )
    #     assert result.returncode == 1

    # @with_beanstalkd()
    # def test_pop_empty(self):
    #     tubename = self.get_tubename()
    #     result =self.run_cli(command='pop', tubename=tubename )
    #     assert result.returncode == 0
    #     assert 'no message available' in str( result.stdout)

    # @with_beanstalkd()
    # def test_peek_no_job_id(self):
    #     tubename = self.get_tubename()
    #     result =self.run_cli(command='peek', tubename=tubename )
    #     assert result.returncode == 1
    #     assert 'invalid job id' in str( result.stderr)

    # @with_beanstalkd()
    # def test_delete_no_job_id(self):
    #     tubename = self.get_tubename()
    #     result =self.run_cli(command='peek', tubename=tubename )
    #     assert result.returncode == 1
    #     assert 'invalid job id' in str( result.stderr)

    # @with_beanstalkd()
    # def test_invalid_command(self):
    #     tubename = self.get_tubename()
    #     result =self.run_cli(command='invalidcode', tubename=tubename , additional_args=[]  )
    #     assert result.returncode != 0
