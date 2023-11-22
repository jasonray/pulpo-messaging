import os
import uuid
import time
import datetime
from pathlib import Path
from statman import Statman



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
        self.log('QueueAdapter init')

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
        self.log('FileQueueAdapter init')
        self._base_path = base_path
        os.makedirs(name=self._base_path, mode=0o777, exist_ok=True)

    def enqueue(self, message: Message) -> Message:
        message_id = self._create_message_id()
        path_file = self._create_new_message_file_path(message_id)
        self._save_message_to_file(message=message, path_file=path_file)
        message._id = message_id
        Statman.gauge('fqa.enqueue').increment()
        return message

    def dequeue(self) -> Message:
        # if there is a message ready for dequeue, return it
        # if no message, return Nothing

        self.log('begin dequeue')

        message_path_file = None
        # entries = os.listdir(path=self._base_path)
        entries = self._get_message_file_list(self._base_path)
        self.log('scanning directory:', entries)
        for file in entries:
            self.log(f'checking file name: {file.name}')

            # in future, this is where I would test for delay and maybe TTL
            self.log('file meets criteria')
            # message_path_file = os.path.join(self._base_path, file)
            self.log(f'attempt to lock message: {file.path}')
            if self._lock_file(file.path):
                self.log('locked message')
                message_path_file = file.path
            else:
                self.log('failed to lock message')
                message_path_file = None

            if message_path_file:
                break

        m = None
        if message_path_file:
            self.log(f'load message: {message_path_file}')
            m = self._load_message_from_file(file_path=message_path_file)
            Statman.gauge('fqa.dequeue').increment()
        else:
            self.log('no message found')

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
    def _save_message_to_file(self, message:Message, path_file:str):
        serialized_message = str(message)
        self.log(f'_save_message_to_file {message.id} [{path_file}]')
        with open(file=path_file, encoding="utf-8", mode='w') as f:
            f.write(serialized_message)

    def _lock_file(self, path_file_name):
        lock_path_file_name = path_file_name + '.lock'
        lock_path = Path(lock_path_file_name)

        self.log('attempt to lock with lock file: ', lock_path_file_name)

        # this is an early way to check if lock already exists
        if os.path.exists(lock_path_file_name):
            self.log('lock exists on message, unable to mark')
            Statman.gauge('fqa.lock-check.exists.check').increment()
            return False

        self.log('touch to create lock')
        try:
            lock_path.touch(exist_ok=False)
        except FileExistsError:
            Statman.gauge('fqa.lock-check.exists.failed-touch').increment()
            self.log('failed to lock, lock already exists')
            return False

        return True

    def _create_message_id(self):
        return f"{time.time()}-{uuid.uuid4()}"

    def _create_new_message_file_path(self, message_id):
        file_name = message_id + '.message'
        path_file = os.path.join(self._base_path, file_name)
        return path_file

    def _get_message_file_list(self, directory) -> os.DirEntry:
        self.log('scanning directory {directory}')
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

        self.log(f'commit {message_id}')
        self._delete_message(message_id=message_id)
        self._delete_lock(message_id=message_id)
        self.log(f'commit complete {message_id}')
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

        self.log(f'rollback {message_id}')
        self._delete_lock(message_id=message_id)
        self.log(f'rollback complete {message_id}')
        Statman.gauge('fqa.rollback').increment()

    def _delete_lock(self, message_id: str):
        self.log(f'remove lock {message_id}')
        lock_file_path = self._get_lock_file_path(message_id=message_id)
        os.remove(lock_file_path)

    def _delete_message(self, message_id: str):
        self.log(f'remove message {message_id}')
        lock_file_path = self._get_message_file_path(message_id=message_id)
        os.remove(lock_file_path)

    def _does_lock_exist(self, messsage_id) -> bool:
        return os.path.exists(self._get_lock_file_path(message_id=messsage_id))

    def _does_message_exist(self, messsage_id) -> bool:
        return os.path.exists(self._get_message_file_path(message_id=messsage_id))
    
    def log(self, *argv):
        message = ""
        for arg in argv:
            if not arg is None:
                message += ' ' + str(arg)

        pid = os.getpid()
        dt = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S.%f")

        TEMPLATE = "[p:{pid}]\t[{dt}]\t{message}"
        output = TEMPLATE.format(pid=pid, dt=dt, message=message)
        # if self.disable_output_buffering:
        print(output, flush=True)
        # else:
        #     self.log(output)

class Kessel():
    _queue_adapter = None

    def __init__(self):
        self.log('init queue adapter')
        self._queue_adapter = FileQueueAdapter('/tmp/kessel/fqa')

    @property
    def queue_adapter(self) -> QueueAdapter:
        return self._queue_adapter
    
    def publish(self, message:Message) -> Message:
        self.log('publish message to queue adapter')
        return self.queue_adapter.enqueue(message)
    
    def initialize(self) -> Message:
        self.log('starting kessel')
        continue_processing=True
        iterations_with_no_messages = 0
        while continue_processing:
            self.log('begin dequeue')
            message = self.queue_adapter.dequeue()
            if message:
                self.log(f'received message {message.id}')
                self.log('this would be the point to delegate to handler')
                self.queue_adapter.commit(message)
                self.log('commit complete')
            else:
                self.log('no message available, sleep')
                iterations_with_no_messages += 1
                if iterations_with_no_messages > 12:
                    self.log('no message available, shutdown')
                    continue_processing=False
                else:
                    self.log('no message available, sleep')
                    time.sleep(5)

    def log(self, *argv):
        message = ""
        for arg in argv:
            if not arg is None:
                message += ' ' + str(arg)

        pid = os.getpid()
        dt = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S.%f")

        TEMPLATE = "[p:{pid}]\t[{dt}]\t{message}"
        output = TEMPLATE.format(pid=pid, dt=dt, message=message)
        # if self.disable_output_buffering:
        print(output, flush=True)
        # else:
        #     self.log(output)

