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
    def history_path(self: Config) -> str:
        return os.path.join(self.base_path, 'history')

    @property
    def message_format(self: Config) -> str:
        return self.get('message_format', 'json')

    @property
    def skip_random_messages_range(self: Config) -> int:
        return self.getAsInt('skip_random_messages_range', 0)

    @property
    def enable_history(self: Config) -> bool:
        return self.getAsBool('enable_history', "False")


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
        os.makedirs(name=self.config.history_path, mode=self.MODE_READ_WRITE, exist_ok=True)

    @property
    def config(self) -> FileQueueAdapterConfig:
        return self._config

    def enqueue(self, message: Message) -> Message:
        message.id = self._create_message_id()
        message_file_path = self._get_message_file_path(message_id=message.id)
        self._save_message_to_file(message=message, path_file=message_file_path)
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

                self.log(f'checking if file on delay {file}')
                m = self._load_message_from_file(file_path=file)

                now = datetime.datetime.now()
                self.log(f'verifying {m.delay=} vs {now=} => {m.delay > now}')
                if m.delay and m.delay > now:
                    self.log('message delayed, do not process yet')
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

    def _load_message_from_file(self, file_path):
        self.log(f'load message [file_path:{file_path}]')
        m = None
        with open(file=file_path, encoding="utf-8", mode='r') as f:
            if self.config.message_format == 'body_only':
                payload = f.read()
                payload = self._trim(payload)
                message_id = self._get_message_id_from_file_path(file_path)
                m = Message(payload=payload)
                self.log(f'load message id from file path [file_path:{file_path}]')
                m.id = message_id
                self.log(f'extracted message id [file_path:{file_path}]=>[id:{m.id}]')
            elif self.config.message_format == 'json':
                message_components = json.load(f)
                print('load from message components', message_components)
                m = Message(components=message_components)
                print(f'loaded {m.id=}')
                self.log(f'loaded message from file {file_path=} m={str(m)}')
            else:
                raise Exception(f'invalid message format config setting {self.config.message_format}')

        return m

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
    def _save_message_to_file(self, message: Message, path_file: str):
        if self.config.message_format == 'body_only':
            serialized_message = message.payload.get('body')
        elif self.config.message_format == 'json':
            self.log(f'save message to file {path_file=} m={str(message)}')
            serialized_message = json.dumps(message._components, indent=2, default=str)
        else:
            raise Exception(f'invalid message format config setting {self.config.message_format}')
        self.log(f'_save_message_to_file [id={message.id}][path={path_file}][format={self.config.message_format}]')
        self.log(f'_save_message_to_file [{serialized_message}]')
        with open(file=path_file, encoding="utf-8", mode='w') as f:
            f.write(serialized_message)

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

    def _get_history_file_path(self, message_id) -> str:
        file_name = f'{message_id}.message'
        path = os.path.join(self.config.history_path, file_name)
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

    def commit(self, message: Message):
        message_id = None
        if isinstance(message, Message):
            message_id = message.id
        elif isinstance(message, str):
            # not really the right way to commit a message but it will work
            message_id = message
        else:
            raise Exception('commit expects message object')

        self.log(f'commit {message_id}')
        if self.config.enable_history:
            self._move_to_history(message_id=message_id)
        else:
            self._delete_lock(message_id=message_id)
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
        self._rollback_lock(message_id=message_id)
        self.log(f'rollback complete [id={message_id}]')
        Statman.gauge('fqa.rollback').increment()

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

    def _move_to_history(self, message_id: str):
        self.log(f'move lock to history {message_id}')
        lock_file_path = self._get_lock_file_path(message_id=message_id)
        message_file_path = self._get_history_file_path(message_id=message_id)
        self.log(f'move file [{lock_file_path}]=>[{message_file_path}]')
        os.rename(src=lock_file_path, dst=message_file_path)

    def _delete_message(self, message_id: str):
        self.log(f'remove message {message_id}')
        lock_file_path = self._get_message_file_path(message_id=message_id)
        os.remove(lock_file_path)

    def _does_lock_exist(self, messsage_id) -> bool:
        return os.path.exists(self._get_lock_file_path(message_id=messsage_id))

    def _does_message_exist(self, messsage_id) -> bool:
        return os.path.exists(self._get_message_file_path(message_id=messsage_id))

    def log(self, *argv):
        logger.log(*argv, flush=True)
