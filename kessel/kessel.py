import os
import uuid
import time
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


class FileQueueAdapter():
    _base_path = None

    def __init__(self, base_path):
        self._base_path = base_path
        os.makedirs(name=self._base_path, mode=0o777, exist_ok=True)

    def enqueue(self, message: Message) -> Message:
        message_id = self.create_message_id()
        path_file = self.create_new_message_path_file(message_id)
        self.save_message_to_file(message=message, path_file=path_file)
        message._id = message_id
        return message

    def dequeue_next(self) -> Message:
        # if there is a message ready for dequeue, return it
        # if no message, return Nothing

        print('begin dequeue')

        message_path_file = None
        # entries = os.listdir(path=self._base_path)
        entries = self.sorted_directory_listing_with_os_scandir(
            self._base_path)
        print('scanning directory:', entries)
        for file in entries:
            print(f'checking file name: {file.name}')

            if file.is_file() and file.name.endswith('.message'):
                # in future, this is where I would test for delay and maybe TTL
                print('file meets criteria')
                # message_path_file = os.path.join(self._base_path, file)
                print(f'attempt to lock message: {file.path}')
                if self.lock_file(file.path):
                    print('locked message')
                    message_path_file = file.path
                else:
                    print('failed to lock message')
                    message_path_file = None
            else:
                print('file does not meet criteria, skipping')

            if message_path_file:
                break

        m = None
        if message_path_file:
            print(f'load message: {message_path_file}')
            m = self.load_message_from_file(path_file=message_path_file)
        else:
            print('no message found')

        return m

    def load_message_by_id(self, message_id):
        return self.load_message_from_file(self.get_message_path(message_id))

    def load_message_from_file(self, path_file):
        m = None
        with open(file=path_file, encoding="utf-8", mode='r') as f:
            header = f.readline()
            header = self.trim(header)
            payload = f.readline()
            payload = self.trim(payload)
        (path, file_name) = os.path.split(path_file)
        message_id = file_name.replace('.message', '')
        m = Message(payload=payload, header=header)
        m._id = message_id
        return m

    # https://docs.python.org/3/tutorial/inputoutput.html#saving-structured-data-with-json
    def save_message_to_file(self, message, path_file):
        serialized_message = str(message)
        with open(file=path_file, encoding="utf-8", mode='w') as f:
            f.write(serialized_message)

    def lock_file(self, path_file_name):
        lock_path_file_name = path_file_name + '.lock'
        lock_path = Path(lock_path_file_name)

        print('attempt to lock with lock file: ', lock_path_file_name)

        # this is an early way to check if lock already exists
        if os.path.exists(lock_path_file_name):
            print('lock exists on message, unable to mark')
            return False

        print('touch to create lock')
        try:
            lock_path.touch()
        except FileExistsError:
            print('failed to lock, lock already exists')
            return False

        return True

    def create_message_id(self):
        return f"{time.time()}-{uuid.uuid4()}"

    def create_new_message_path_file(self, message_id):
        file_name = message_id + '.message'
        path_file = os.path.join(self._base_path, file_name)
        return path_file

    def sorted_directory_listing_with_os_scandir(self, directory):
        with os.scandir(directory) as entries:
            sorted_entries = sorted(entries, key=lambda entry: entry.name)
            # sorted_items = [entry.name for entry in sorted_entries]
        # return DirEntry https://docs.python.org/3/library/os.html#os.DirEntry
        return sorted_entries

    def get_message_path(self, message_id) -> str:
        file_name = f'{message_id}.message'
        path = os.path.join(self._base_path, file_name)
        return path

    def get_lock_path(self, message_id) -> str:
        return self.get_message_path(message_id=message_id) + '.lock'

    def does_lock_exist(self, messsage_id) -> bool:
        return os.path.exists(self.get_lock_path(message_id=messsage_id))

    def does_message_exist(self, messsage_id) -> bool:
        return os.path.exists(self.get_message_path(message_id=messsage_id))

    def trim(self, text):
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

        message_file_path = self.get_message_path(message_id=message_id)
        print(f'remove message {message_file_path}')
        os.remove(message_file_path)

        lock_file_path = self.get_lock_path(message_id=message_id)
        print(f'remove lock {lock_file_path}')
        os.remove(lock_file_path)

        print('commit complete')

    def rollback(self, message: Message):
        message_id = None
        if isinstance(message, Message):
            message_id = message.id
        elif isinstance(message, str):
            # not really the right way to commit a message but it will work
            message_id = message
        else:
            raise Exception('commit expects message object')

        lock_file_path = self.get_lock_path(message_id=message_id)
        print(f'remove lock {lock_file_path}')
        os.remove(lock_file_path)

        print('rollback complete')
