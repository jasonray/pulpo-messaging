import os
import uuid
import time
import datetime
import json
import random
from statman import Statman
from pulpo_config import Config


class Message():
    # - priority
    # - delay
    # - ttr
    # - payload

    _id = None
    _payload = None
    _request_type = None
    _header = None

    def __init__(self, payload=None, header=None, request_type=None):
        self._payload = payload
        self._header = header
        self._request_type = request_type

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
    def request_type(self):
        return self._request_type

    @property
    def payload(self):
        return self._payload


class PayloadHandler():
    _config = None

    def __init__(self, options: dict = None):
        self._config = Config(options=options)

    def handle(self, payload: str):
        pass

    @property
    def config(self) -> Config:
        return self._config


class EchoHandler(PayloadHandler):

    def __init__(self, options: dict = None):
        super().__init__(options=options)
        os.makedirs(name=self.destination_directory, mode=0o777, exist_ok=True)

    def handle(self, payload: str):
        print('EchoHandler.handle')
        destination_filename = f'{uuid.uuid4()}.echo.txt'
        destination_file_path = os.path.join(self.destination_directory, destination_filename)
        with open(file=destination_file_path, encoding="utf-8", mode='w') as f:
            f.write(payload)
        return payload

    @property
    def destination_directory(self) -> str:
        return self.config.get('destination_directory', '/tmp/kessel/EchoHandler-output')


class UpperCaseHandler(EchoHandler):

    def handle(self, payload: str):
        print('UpperCaseHandler.handle')
        EchoHandler.handle(self, payload.upper())


class LowerCaseHandler(EchoHandler):

    def handle(self, payload: str):
        print('LowerCaseHandler.handle')
        EchoHandler.handle(self, payload.lower())


class QueueAdapter():

    def enqueue(self, message: Message) -> Message:
        pass

    def dequeue(self) -> Message:
        pass

    def commit(self, message: Message) -> Message:
        pass

    def rollback(self, message: Message) -> Message:
        pass


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
        return self.get('message_format', 'body_only')

    @property
    def skip_random_messages_range(self: Config) -> int:
        return self.getAsInt('skip_random_messages_range', 0)

    @property
    def enable_history(self: Config) -> bool:
        return self.getAsBool('enable_history', "False")


class FileQueueAdapter(QueueAdapter):
    _config = None

    def __init__(self, options: dict):
        super().__init__()
        self.log('FileQueueAdapter init')

        self._config = FileQueueAdapterConfig(options)
        self._create_message_directories()

    def _create_message_directories(self):
        os.makedirs(name=self.config.base_path, mode=0o777, exist_ok=True)
        os.makedirs(name=self.config.lock_path, mode=0o777, exist_ok=True)
        os.makedirs(name=self.config.history_path, mode=0o777, exist_ok=True)

    @property
    def config(self) -> FileQueueAdapterConfig:
        return self._config

    def enqueue(self, message: Message) -> Message:
        message_id = self._create_message_id()
        message._id = message_id
        message_file_path = self._get_message_file_path(message_id=message_id)
        self._save_message_to_file(message=message, path_file=message_file_path)
        self.log(f'fqa.enqueue [id={message._id}][file_path={message_file_path}]')
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
                m._id = message_id
                self.log(f'extracted message id [file_path:{file_path}]=>[id:{message_id}]')
            elif self.config.message_format == 'json':
                message_parts = json.load(f)
                payload = message_parts['payload']
                message_id = message_parts['id']
                header = message_parts['header']
                type = message_parts['type']
                m = Message(payload=payload, header=header, type=type)
                m._id = message_id
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
            serialized_message = message.payload
        elif self.config.message_format == 'json':
            message_parts = {}
            message_parts['id'] = message.id
            message_parts['header'] = message.header
            message_parts['type'] = message.request_type
            message_parts['payload'] = message.payload
            serialized_message = json.dumps(message_parts, indent=2)
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
            # not really the right way to commit a message but it will work
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


class HandlerRegistry():
    _registry = None

    def __init__(self):
        self._registry = {}

    def register(self, type: str, handler: PayloadHandler):
        self._registry[type] = handler

    def get(self, type: str) -> PayloadHandler:
        return self._registry.get(type)


class KesselConfig(Config):

    def __init__(self, options: dict = None, json_file_path: str = None):
        super().__init__(options=options, json_file_path=json_file_path)

    @property
    def shutdown_after_number_of_empty_iterations(self) -> int:
        return self.getAsInt('shutdown_after_number_of_empty_iterations', 5)

    @property
    def queue_adapter_type(self) -> str:
        return self.get('queue_adapter_type', None)

    @property
    def sleep_duration(self) -> int:
        return self.getAsInt('sleep_duration', 5)

    @property
    def enable_output_buffering(self) -> bool:
        return self.getAsBool(key='enable_output_buffering', default_value=False)


class Kessel():
    _queue_adapter = None
    _config = None
    _handler_registry = None

    def __init__(self, options: dict):
        self.log('init queue adapter')
        self._config = KesselConfig(options)

        if self.config.queue_adapter_type == 'FileQueueAdapter':
            self._queue_adapter = FileQueueAdapter(self.config.get('file_queue_adapter'))
        else:
            raise Exception('invalid queue adapter type')

        self._handler_registry = HandlerRegistry()
        self.handler_registry.register('echo', EchoHandler(self.config.get('echo_handler')))
        self.handler_registry.register('lower', LowerCaseHandler(self.config.get('lower_handler')))
        self.handler_registry.register('upper', UpperCaseHandler(self.config.get('upper_handler')))

    @property
    def config(self) -> KesselConfig:
        return self._config

    @property
    def queue_adapter(self) -> QueueAdapter:
        return self._queue_adapter

    @property
    def handler_registry(self) -> HandlerRegistry:
        return self._handler_registry

    def publish(self, message: Message) -> Message:
        self.log('publish message to queue adapter')
        return self.queue_adapter.enqueue(message)

    def initialize(self) -> Message:
        self.log('starting kessel')
        continue_processing = True
        iterations_with_no_messages = 0
        Statman.stopwatch('kessel.message_streak_tm', autostart=True)
        Statman.calculation(
            'kessel.message_streak_messages_per_s').calculation_function = lambda: Statman.gauge('kessel.message_streak_cnt').value / Statman.stopwatch('kessel.message_streak_tm').value
        while continue_processing:
            self.log('kessel init begin dequeue')

            message = self.queue_adapter.dequeue()
            Statman.gauge('kessel.dequeue-attempts').increment()
            if message:
                iterations_with_no_messages = 0
                Statman.gauge('kessel.dequeue').increment()
                Statman.gauge('kessel.message_streak_cnt').increment()
                self.log(f'received message {message.id} {message.request_type}')

                handler = self.handler_registry.get(message.request_type)
                self.log(f'handler: {handler}')
                if handler is None:
                    self.log(f'WARNING no handler for message type {message.request_type}')
                else:
                    handler.handle(payload=message.payload)

                self.queue_adapter.commit(message)
                Statman.gauge('kessel.messages_processed').increment()
                self.log('commit complete')
            else:
                iterations_with_no_messages += 1
                self.log(f'no message available [iteration with no messages = {iterations_with_no_messages}][max = {self.config.shutdown_after_number_of_empty_iterations}]')

                Statman.stopwatch('kessel.message_streak_tm').stop()
                Statman.report(output_stdout=False, log_method=self.log)

                if iterations_with_no_messages >= self.config.shutdown_after_number_of_empty_iterations:
                    self.log('no message available, shutdown')
                    continue_processing = False
                else:
                    self.log('no message available, sleep')
                    time.sleep(self.config.sleep_duration)
                    Statman.stopwatch('kessel.message_streak_tm').start()
                    Statman.gauge('kessel.message_streak_cnt').value = 0

    def log(self, *argv):
        message = ""
        for arg in argv:
            if not arg is None:
                message += ' ' + str(arg)

        pid = os.getpid()
        dt = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S.%f")

        TEMPLATE = "[p:{pid}]\t[{dt}]\t{message}"
        output = TEMPLATE.format(pid=pid, dt=dt, message=message)

        flush = False
        if self.config and not self.config.enable_output_buffering:
            flush = True

        print(output, flush=flush)
