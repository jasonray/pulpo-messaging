import os
import uuid
import time
from statman import Statman
from pathlib import Path


class Message():
    # - priority
    # - delay
    # - ttr
    # - payload

    _id = None
    _payload = None
    _header = None

    def __init__(self, payload=None, header=None):
        self._payload = payload
        self._header = header

    def __str__(self):
        serialized = ''
        serialized += 'm1.0 \n'
        serialized += '' + self.payload
        return serialized

    @property
    def id(self):
        return self._id

    @property
    def header(self):
        return self._header

    @property
    def payload(self):
        return self._payload


class QueueAdapter():

    def __init__(self):
        print('QueueAdapter init')

    def enqueue(self, message: Message) -> Message:
        pass

    def dequeue(self) -> Message:
        pass

    def commit(self, message: Message) -> Message:
        pass

    def rollback(self, message: Message) -> Message:
        pass


class FileQueueAdapter(QueueAdapter):
    _base_path = None

    def __init__(self, base_path):
        super().__init__()
        print('FileQueueAdapter init')
        self._base_path = base_path
        os.makedirs(name=self._base_path, mode=0o777, exist_ok=True)

    def enqueue(self, message: Message) -> Message:
        message_id = self._create_message_id()
        path_file = self._create_new_message_file_path(message_id)
        self._save_message_to_file(message=message, path_file=path_file)
        message._id = message_id
        Statman.gauge('fqa.enqueue').increment()
        return message

    def dequeue_next(self) -> Message:
        # if there is a message ready for dequeue, return it
        # if no message, return Nothing

        print('begin dequeue')

        message_path_file = None
        # entries = os.listdir(path=self._base_path)
        entries = self._get_message_file_list(self._base_path)
        print('scanning directory:', entries)
        for file in entries:
            print(f'checking file name: {file.name}')

            # in future, this is where I would test for delay and maybe TTL
            print('file meets criteria')
            # message_path_file = os.path.join(self._base_path, file)
            print(f'attempt to lock message: {file.path}')
            if self._lock_file(file.path):
                print('locked message')
                message_path_file = file.path
            else:
                print('failed to lock message')
                message_path_file = None

            if message_path_file:
                break

        m = None
        if message_path_file:
            print(f'load message: {message_path_file}')
            m = self._load_message_from_file(file_path=message_path_file)
            Statman.gauge('fqa.dequeue').increment()
        else:
            print('no message found')

        return m

    def load_message_by_id(self, message_id):
        return self._load_message_from_file(self._get_message_file_path(message_id))

    def _load_message_from_file(self, file_path):
        m = None
        with open(file=file_path, encoding="utf-8", mode='r') as f:
            header = f.readline()
            header = self._trim(header)
            payload = f.readline()
            payload = self._trim(payload)

        message_id = self._get_message_id_from_file_path(file_path)
        m = Message(payload=payload, header=header)
        m._id = message_id
        return m

    def _get_message_id_from_file_path(self, message_file_path):
        (message_path, message_file_name) = os.path.split(message_file_path)
        return self._get_message_id_from_file_name(message_file_name)

    def _get_message_id_from_file_name(self, message_file_name):
        # todo: pretty weak approach, might try to get better way
        # like message id in message
        return message_file_name.replace('.message', '')

    # https://docs.python.org/3/tutorial/inputoutput.html#saving-structured-data-with-json
    def _save_message_to_file(self, message, path_file):
        serialized_message = str(message)
        with open(file=path_file, encoding="utf-8", mode='w') as f:
            f.write(serialized_message)

    def _lock_file(self, path_file_name):
        lock_path_file_name = path_file_name + '.lock'
        lock_path = Path(lock_path_file_name)

        print('attempt to lock with lock file: ', lock_path_file_name)

        # this is an early way to check if lock already exists
        if os.path.exists(lock_path_file_name):
            print('lock exists on message, unable to mark')
            Statman.gauge('fqa.lock-check.exists.check').increment()
            return False

        print('touch to create lock')
        try:
            lock_path.touch()
        except FileExistsError:
            Statman.gauge('fqa.lock-check.exists.failed-touch').increment()
            print('failed to lock, lock already exists')
            return False

        return True

    def _create_message_id(self):
        return f"{time.time()}-{uuid.uuid4()}"

    def _create_new_message_file_path(self, message_id):
        file_name = message_id + '.message'
        path_file = os.path.join(self._base_path, file_name)
        return path_file

    def _get_message_file_list(self, directory) -> os.DirEntry:
        with os.scandir(directory) as entries:
            sorted_entries = sorted(entries, key=lambda entry: entry.name)

        filtered_entries = filter(lambda entry: entry.name.endswith('.message'), sorted_entries)
        filtered_entries = filter(lambda entry: entry.is_file(), filtered_entries)

        # return DirEntry https://docs.python.org/3/library/os.html#os.DirEntry
        return filtered_entries

    def _get_message_file_path(self, message_id) -> str:
        file_name = f'{message_id}.message'
        path = os.path.join(self._base_path, file_name)
        return path

    def _get_lock_file_path(self, message_id) -> str:
        return self._get_message_file_path(message_id=message_id) + '.lock'

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

        print(f'commit {message_id}')
        self._delete_message(message_id=message_id)
        self._delete_lock(message_id=message_id)
        print(f'commit complete {message_id}')
        Statman.gauge('fqa.commit').increment()

    def rollback(self, message: Message):
        message_id = None
        if isinstance(message, Message):
            message_id = message.id
        elif isinstance(message, str):
            # not really the right way to commit a message but it will work
            message_id = message
        else:
            raise Exception('rollback expects message object')

        print(f'rollback {message_id}')
        self._delete_lock(message_id=message_id)
        print(f'rollback complete {message_id}')
        Statman.gauge('fqa.rollback').increment()

    def _delete_lock(self, message_id: str):
        print(f'remove lock {message_id}')
        lock_file_path = self._get_lock_file_path(message_id=message_id)
        os.remove(lock_file_path)

    def _delete_message(self, message_id: str):
        print(f'remove message {message_id}')
        lock_file_path = self._get_message_file_path(message_id=message_id)
        os.remove(lock_file_path)

    def _does_lock_exist(self, messsage_id) -> bool:
        return os.path.exists(self._get_lock_file_path(message_id=messsage_id))

    def _does_message_exist(self, messsage_id) -> bool:
        return os.path.exists(self._get_message_file_path(message_id=messsage_id))
