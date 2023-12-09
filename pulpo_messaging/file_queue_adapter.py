import os
import uuid
import time
import json
import random
import datetime
from statman import Statman
from pulpo_config import Config
from pulpo_messaging import logger
from pulpo_messaging.message import Message
from pulpo_messaging.queue_adapter import QueueAdapter


class FileQueueAdapterConfig(Config):

    def __init__(self, options: dict = None, json_file_path: str = None):
        super().__init__(options=options, json_file_path=json_file_path)

    @property
    def base_path(self: Config) -> str:
        return self.get('base_path', '/tmp/kessel/default')

    @property
    def lock_path(self: Config) -> str:
        return os.path.join(self.base_path, 'lock')

    @property
    def history_success_path(self: Config) -> str:
        return os.path.join(self.base_path, 'history', 'success')

    @property
    def history_failure_path(self: Config) -> str:
        return os.path.join(self.base_path, 'history', 'failure')

    @property
    def message_format(self: Config) -> str:
        return self.get('message_format', 'json')

    @property
    def skip_random_messages_range(self: Config) -> int:
        return self.getAsInt('skip_random_messages_range', 0)

    @property
    def enable_history(self: Config) -> bool:
        return self.getAsBool('enable_history', "False")

    @property
    def max_number_of_attempts(self: Config) -> bool:
        return self.getAsInt('max_number_of_attempts', 0)


class FileQueueAdapter(QueueAdapter):
    MODE_READ_WRITE = 0o770
    MODE_READ_WRITE_EXECUTE = 0o777

    _config = None

    def __init__(self, options: dict):
        super().__init__()
        self.log('FileQueueAdapter init')

        self._config = FileQueueAdapterConfig(options)
        self._create_message_directories()

    def _create_message_directories(self):
        os.makedirs(name=self.config.base_path, mode=self.MODE_READ_WRITE, exist_ok=True)
        os.makedirs(name=self.config.lock_path, mode=self.MODE_READ_WRITE, exist_ok=True)
        os.makedirs(name=self.config.history_success_path, mode=self.MODE_READ_WRITE, exist_ok=True)
        os.makedirs(name=self.config.history_failure_path, mode=self.MODE_READ_WRITE, exist_ok=True)

    @property
    def config(self) -> FileQueueAdapterConfig:
        return self._config

    def enqueue(self, message: Message) -> Message:
        message.id = self._create_message_id()
        message_file_path = self._get_message_file_path(message_id=message.id)
        self._save_message_to_file(message=message, file_path=message_file_path)
        self.log(f'fqa.enqueue [id={message.id}][file_path={message_file_path}]')
        Statman.gauge('fqa.enqueue').increment()
        return message

    def dequeue(self) -> Message:
        # if there is a message ready for dequeue, return it
        # if no message, return Nothing

        self.log('begin dequeue')

        lock_file_path = None
        entries = self._get_message_file_list(self.config.base_path)

        skip_messages = random.randint(0, self.config.skip_random_messages_range)
        i = 0
        last_file = None

        for file in entries:
            # self.log(f'checking file name: {file.name}')
            # # in future, this is where I would test for delay and maybe TTL
            # self.log('file meets criteria')
            # message_path_file = os.path.join(self.config.base_path, file)

            if (i >= skip_messages):
                last_file = None

                m = self._load_message_from_file(file_path=file)
                self.log(f'loaded message [{m.id=}][{m.delay=}[]{m.attempts=}][{file.path=}][{m.expiration=}]')

                now = datetime.datetime.now()
                if m.delay and m.delay > now:
                    self.log('message delayed, do not process yet')
                    # todo: should move this out of queue
                elif self.config.max_number_of_attempts and m.attempts >= self.config.max_number_of_attempts:
                    self.log(f'message exceed max attempts {self.config.max_number_of_attempts=} {m.attempts=}')
                    self._move_to_history(message_id=m.id, source='queue', destination='failure')
                elif m.expiration and m.expiration < now:
                    self.log(f'message expired {m.expiration=}')
                    self._move_to_history(message_id=m.id, source='queue', destination='failure')
                else:
                    self.log(f'attempt to lock message: {file.path}')
                    lock_file_path = self._lock_file(file.path)
                    if lock_file_path:  # pylint: disable=no-else-break
                        self.log('locked message')
                        break
                    else:
                        self.log('failed to lock message')
            else:
                # self.log(f'skip message [i={i}][skip={skip_messages}]')
                last_file = file

            i += 1

        if last_file and not lock_file_path:
            file = last_file
            self.log('skipped all messages, trying last message from loop')
            self.log(f'attempt to lock message: {file.path}')
            lock_file_path = self._lock_file(file.path)
            if lock_file_path:
                self.log('locked message')
            else:
                self.log('failed to lock message')

        m = None
        if lock_file_path:
            self.log(f'load message (dq): {lock_file_path}')
            m = self._load_message_from_file(file_path=lock_file_path)
            Statman.gauge('fqa.dequeue').increment()
        else:
            self.log('no message found')

        return m

    def load_message_by_id(self, message_id):
        return self._load_message_from_file(self._get_message_file_path(message_id))

    def _load_message_from_file(self, file_path) -> Message:
        self.log(f'load message from file [{file_path=}][format={self.config.message_format}]')
        message = None
        with open(file=file_path, encoding="utf-8", mode='r') as f:
            if self.config.message_format == 'json':
                message_components = json.load(f)
                message = Message(components=message_components)
            else:
                raise Exception(f'invalid message format config setting {self.config.message_format}')

        self.log(f'loaded message from file [{file_path=}][{message.id=}')
        return message

    def _get_message_id_from_file_path(self, message_file_path):
        (message_path, message_file_name) = os.path.split(message_file_path)  #pylint: disable=unused-variable
        return self._get_message_id_from_file_name(message_file_name)

    def _get_message_id_from_file_name(self, message_file_name):
        # todo: pretty weak approach, might try to get better way
        # like message id in message
        buffer = message_file_name
        buffer = buffer.replace('.message', '')
        buffer = buffer.replace('.lock', '')
        return buffer

    # https://docs.python.org/3/tutorial/inputoutput.html#saving-structured-data-with-json
    def _save_message_to_file(self, message: Message, file_path: str):
        self.log(f'save message [id={message.id}][path={file_path}][format={self.config.message_format}]')
        if self.config.message_format == 'json':
            serialized_message = json.dumps(message._components, indent=2, default=str)
        else:
            raise Exception(f'invalid message format config setting {self.config.message_format}')
        with open(file=file_path, encoding="utf-8", mode='w') as f:
            f.write(serialized_message)
        self.log(f'saved message [{file_path=}][id={message.id}]')

    def _lock_file(self, message_file_path) -> str:
        (message_path, message_file_name) = os.path.split(message_file_path)
        lock_file_path = os.path.join(self.config.lock_path, message_file_name + '.lock')
        self.log(f'_lock_file [message_path={message_path}][message_file_name={message_file_name}][lock_file_path={lock_file_path}]')

        try:
            self.log(f'attempt to lock with lock file: [{message_file_path}]=>[{lock_file_path}]')
            os.rename(src=message_file_path, dst=lock_file_path)
        except FileExistsError:
            Statman.gauge('fqa.lock-check.exists.failed-lock.FileExistsError').increment()
            self.log('failed to lock (FileExistsError)')
            return None
        except FileNotFoundError:
            Statman.gauge('fqa.lock-check.exists.failed-lock.FileNotFoundError').increment()
            self.log('failed to lock (FileNotFoundError)')
            return None

        return lock_file_path

    def _create_message_id(self):
        return f"{time.time()}-{uuid.uuid4()}"

    def _get_message_file_list(self, directory) -> os.DirEntry:
        self.log(f'scanning directory {directory}')
        with os.scandir(directory) as entries:
            sorted_entries = sorted(entries, key=lambda entry: entry.name)

        filtered_entries = filter(lambda entry: entry.name.endswith('.message'), sorted_entries)
        filtered_entries = filter(lambda entry: entry.is_file(), filtered_entries)

        # return DirEntry https://docs.python.org/3/library/os.html#os.DirEntry
        return filtered_entries

    def _get_message_file_path(self, message_id) -> str:
        file_name = f'{message_id}.message'
        path = os.path.join(self.config.base_path, file_name)
        self.log(f'_get_message_file_path [id:{message_id}]=>[file_name:{file_name}]=>[path:{path}]')
        return path

    def _does_history_success_message_exist(self, message_id  ) -> bool:
        return os.path.exists(self._get_history_file_path_success(message_id=message_id))

    def _get_history_file_path_success(self, message_id) -> str:
        file_name = f'{message_id}.message'
        path = os.path.join(self.config.history_success_path, file_name)
        self.log(f'_get_history_file_path [id:{message_id}]=>[file_name:{file_name}]=>[path:{path}]')
        return path

    def _does_history_failure_message_exist(self, message_id) -> bool:
        return os.path.exists(self._get_history_file_path_failure(message_id=message_id))

    def _get_history_file_path_failure(self, message_id) -> str:
        file_name = f'{message_id}.message'
        path = os.path.join(self.config.history_failure_path, file_name)
        self.log(f'_get_history_file_path [id:{message_id}]=>[file_name:{file_name}]=>[path:{path}]')
        return path

    def _get_lock_file_path(self, message_id) -> str:
        file_name = f'{message_id}.message.lock'
        path = os.path.join(self.config.lock_path, file_name)
        self.log(f'_get_lock_file_path [id:{message_id}]=>[file_name:{file_name}]=>[path:{path}]')
        return path

    def _trim(self, text):
        text = str(text)
        text = text.replace('\r', '')
        text = text.replace('\n', '')
        text = text.strip()
        return text

    def commit(self, message: Message, is_success: bool = True):
        message_id = None
        if isinstance(message, Message):
            message_id = message.id
        elif isinstance(message, str):
            # not really the right way to commit a message but it will work
            message_id = message
        else:
            raise Exception('commit expects message object')

        self.log(f'commit {message_id}')
        self._move_to_history(message_id=message_id, source='lock', destination='success' if is_success else 'failure')
        self.log(f'commit complete {message_id}')
        Statman.gauge('fqa.commit').increment()

    def rollback(self, message: Message):
        message_id = None
        if isinstance(message, Message):
            message_id = message.id
        elif isinstance(message, str):
            message_id = message
        else:
            raise Exception('rollback expects message object')

        self.log(f'rollback [id={message_id}]')
        self._increment_failed_attempts(message_id=message_id)
        self._rollback_lock(message_id=message_id)
        self.log(f'rollback complete [id={message_id}]')
        Statman.gauge('fqa.rollback').increment()

    def _increment_failed_attempts(self, message_id: str):
        '''Increments the attempts counter in the message.  This method assumes the message is in the lock directory.'''
        lock_file_path = self._get_lock_file_path(message_id=message_id)
        m = self._load_message_from_file(file_path=lock_file_path)
        m.attempts += 1
        self._save_message_to_file(message=m, file_path=lock_file_path)

    def _rollback_lock(self, message_id: str):
        self.log(f'rollback lock [id={message_id}]')
        lock_file_path = self._get_lock_file_path(message_id=message_id)
        message_file_path = self._get_message_file_path(message_id=message_id)
        self.log(f'move file [{lock_file_path}]=>[{message_file_path}]')
        os.rename(src=lock_file_path, dst=message_file_path)

    def _delete_lock(self, message_id: str):
        self.log(f'remove lock {message_id}')
        lock_file_path = self._get_lock_file_path(message_id=message_id)
        self.log(f'delete file {lock_file_path}')
        os.remove(lock_file_path)

    def _move_to_history(self, message_id: str, source: str, destination: str):
        '''
        Move message to history.
        Source: queue | lock
        Destination: success | failure
        '''

        self.log(f'archive message [{message_id=}][{source=}][{destination=}]')
        source_file_path = None
        destination_file_path = None

        if source == 'queue':
            source_file_path = self._get_message_file_path(message_id=message_id)
        elif source == 'lock':
            source_file_path = self._get_lock_file_path(message_id=message_id)
        else:
            raise Exception(f'Unable to move message.  Invalid source {source=}')

        if destination == 'success':
            destination_file_path = self._get_history_file_path_success(message_id=message_id)
        elif destination == 'failure':
            destination_file_path = self._get_history_file_path_failure(message_id=message_id)
        else:
            raise Exception(f'Unable to move message.  Invalid destination {destination=}')

        self.log(f'move file [{source_file_path}]=>[{destination_file_path}]')
        os.rename(src=source_file_path, dst=destination_file_path)

    def _delete_message(self, message_id: str):
        self.log(f'remove message {message_id}')
        lock_file_path = self._get_message_file_path(message_id=message_id)
        os.remove(lock_file_path)

    def _does_lock_exist(self, message_id) -> bool:
        return os.path.exists(self._get_lock_file_path(message_id=message_id))

    def _does_message_exist(self, message_id) -> bool:
        return os.path.exists(self._get_message_file_path(message_id=message_id))

    def lookup_message_state(self, message_id):
        '''
        Utility to determine state based upon message location.  This is expected to be used for informative purposes only, and NOT for message flow.
        Valid states: unknown, queue, lock, complete.success, complete.fail
        '''
        self.log(f'lookup message state [{message_id=}][{self.config.base_path=}]')
        state = None
        if not state and self._does_message_exist(message_id=message_id):
            state = 'queue'
        if not state and self._does_lock_exist(message_id=message_id):
            state = 'lock'
        if not state and self._does_history_success_message_exist(message_id=message_id):
            state = 'complete.success'
        if not state and self._does_history_failure_message_exist(message_id=message_id):
            state = 'complete.fail'
        if not state:
            state = 'unknown'
        self.log(f'lookup message state [{message_id=}]=>[{state=}]')
        return state

    def log(self, *argv):
        logger.log(*argv, flush=True)
