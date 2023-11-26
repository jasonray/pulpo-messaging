import os
import uuid
import time
import datetime
import typing
import json
import argparse
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


class Config():
    __options = None

    def __init__(self, options: dict = None, json_file_path: str = None):
        if not options and json_file_path:
            options = self._load_options_from_file(json_file_path=json_file_path)
        elif not options:
            options = {}

        if isinstance(options, dict):
            self.__options = options
        elif isinstance(options, Config):
            self.__options = options.__options

    def process_args(self, args: dict):
        if args:
            print(f'processing args [{args}]')
            if isinstance(args, argparse.ArgumentParser):
                args = args.parse_args()
                print(f'process command line arguments [{args}]')
            if isinstance(args, argparse.Namespace):
                args = vars(args)
                print(f'converted args to dictionary [{args}]')

            for arg in args:
                print(f'processing args [arg={arg}]')
                # value = getattr(args, arg)
                value = args.get(arg)
                print(f'processing args [arg={arg}][value={value}]')
                if value:
                    print(f'set config [key={arg}][value={value}]')
                    self.set(arg, value)

    def _load_options_from_file(self, json_file_path: str = None) -> dict:
        options = None
        with open(json_file_path, "rb") as f:
            options = json.load(f)
        return options

    def get(self, key: str, default_value: typing.Any = None):
        keys = key.split('.')

        value = self.__options
        for subkey in keys:
            if value:
                if subkey in value:
                    value = value[subkey]
                else:
                    value = None
            else:
                value = None

        if not value:
            value = default_value

        return value

    # support key=a.b.c where it will create intermediate dictionaries
    def set(self, key: str, value: typing.Any):
        print('options.set')
        keys = key.split('.')

        parent = self.__options
        print('options', self.__options)
        print(f'keys [keys:{keys}][key count:{len(keys)}]')
        for key_number in range(0, len(keys) - 1):
            key = keys[key_number]
            print(f'iterate keys [key_num:{key_number}][key={key}][parent={parent}]')
            if not key in parent:
                print(f'init item [key={key}][parent={parent}]')
                parent[key] = {}
                print(f'init item complete [key={key}][parent={parent}]')
            parent = parent.get(key)
            print(f'new parent [parent={parent}]')
            print('options l', self.__options)

        last_key = keys[len(keys) - 1]
        print(f'set parent to value [parent={parent}][last_key={last_key}][value={value}]')
        parent[last_key] = value
        print('options', self.__options)


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
    def message_format(self: Config) -> str:
        return self.get('message_format', 'body_only')


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

        for file in entries:
            # self.log(f'checking file name: {file.name}')
            # # in future, this is where I would test for delay and maybe TTL
            # self.log('file meets criteria')
            # message_path_file = os.path.join(self.config.base_path, file)

            self.log(f'attempt to lock message: {file.path}')
            lock_file_path = self._lock_file(file.path)
            if lock_file_path:  # pylint: disable=no-else-break
                self.log('locked message')
                break
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
        if self.config.message_format == 'body_only':
            with open(file=file_path, encoding="utf-8", mode='r') as f:
                payload = f.read()
                payload = self._trim(payload)
        else:
            raise Exception(f'invalid message format config setting {self.config.message_format}')

        self.log(f'load message id from file path [file_path:{file_path}]')
        message_id = self._get_message_id_from_file_path(file_path)
        self.log(f'extracted message id [file_path:{file_path}]=>[id:{message_id}]')
        m = Message(payload=payload)
        m._id = message_id
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
        else:
            raise Exception(f'invalid message format config setting {self.config.message_format}')
        self.log(f'_save_message_to_file [id={message.id}][{path_file}]')
        with open(file=path_file, encoding="utf-8", mode='w') as f:
            f.write(serialized_message)

    def _lock_file(self, message_file_path) -> str:
        (message_path, message_file_name) = os.path.split(message_file_path)
        lock_file_path = os.path.join(self.config.lock_path, message_file_name + '.lock')
        self.log(
            f'_lock_file [message_path={message_path}][message_file_name={message_file_name}][lock_file_path={lock_file_path}]'
        )

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
        self.log(
            f'_get_message_file_path [id:{message_id}]=>[file_name:{file_name}]=>[path:{path}]')
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
        # self._delete_message(message_id=message_id)
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


class KesselConfig(Config):

    def __init__(self, options: dict = None, json_file_path: str = None):
        super().__init__(options=options, json_file_path=json_file_path)

    @property
    def shutdown_after_number_of_empty_iterations(self) -> int:
        return self.get('shutdown_after_number_of_empty_iterations', 5)

    @property
    def queue_adapter_type(self) -> str:
        return self.get('queue_adapter_type', None)

    @property
    def sleep_duration(self) -> int:
        return self.get('sleep_duration', 5)

    @property
    def enable_output_buffering(self) -> bool:
        return self.get(key='enable_output_buffering', default_value=False)


class Kessel():
    _queue_adapter = None
    _config = None

    def __init__(self, options: dict):
        self.log('init queue adapter')
        self._config = KesselConfig(options)

        if self.config.queue_adapter_type == 'FileQueueAdapter':
            self._queue_adapter = FileQueueAdapter(self.config.get('file_queue_adapter'))
        else:
            raise Exception('invalid queue adapter type')

    @property
    def config(self) -> KesselConfig:
        return self._config

    @property
    def queue_adapter(self) -> QueueAdapter:
        return self._queue_adapter

    def publish(self, message: Message) -> Message:
        self.log('publish message to queue adapter')
        return self.queue_adapter.enqueue(message)

    def initialize(self) -> Message:
        self.log('starting kessel')
        continue_processing = True
        iterations_with_no_messages = 0
        Statman.stopwatch('kessel.message_streak_tm', autostart=True)
        while continue_processing:
            self.log('kessel init begin dequeue')

            message = self.queue_adapter.dequeue()
            Statman.gauge('kessel.dequeue-attempts').increment()
            if message:
                Statman.gauge('kessel.dequeue').increment()
                Statman.gauge('kessel.message_streak_cnt').increment()
                self.log(f'received message {message.id}')
                self.log('this would be the point to delegate to handler')
                self.queue_adapter.commit(message)
                Statman.gauge('kessel.messages_processed').increment()
                self.log('commit complete')
            else:
                iterations_with_no_messages += 1
                self.log(
                    f'no message available [iteration with no messages = {iterations_with_no_messages}][max = {self.config.shutdown_after_number_of_empty_iterations}]'
                )

                Statman.stopwatch('kessel.message_streak_tm').stop()
                self.print_metrics()

                if iterations_with_no_messages >= self.config.shutdown_after_number_of_empty_iterations:
                    self.log('no message available, shutdown')
                    continue_processing = False
                else:
                    self.log('no message available, sleep')
                    time.sleep(self.config.sleep_duration)
                    Statman.gauge('kessel.message_streak_cnt').value = 0
                    Statman.stopwatch('kessel.message_streak_tm').reset()
                    Statman.stopwatch('kessel.message_streak_tm').start()

    def print_metrics(self):
        self.log('kessel metric report:')
        self.print_metric('kessel.messages_processed')
        self.print_metric('kessel.dequeue-attempts')
        self.print_metric('kessel.dequeue')
        self.print_metric('fqa.lock-check.exists.failed-lock.FileExistsError')
        self.print_metric('fqa.lock-check.exists.failed-lock.FileNotFoundError')
        self.print_metric('kessel.message_streak_cnt')
        self.print_metric('kessel.message_streak_tm')

    def print_metric(self, metric_name):
        self.log('- ' + str(Statman.get(metric_name)))

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
