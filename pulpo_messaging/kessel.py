import time
from statman import Statman
from pulpo_config import Config
from art import text2art
from pulpo_messaging import logger
from .file_queue_adapter import FileQueueAdapter
from .message import Message
from .payload_handler import PayloadHandler
from .queue_adapter import QueueAdapter
from .sample_handlers import EchoHandler, LowerCaseHandler, UpperCaseHandler


class HandlerRegistry():
    _registry = None

    def __init__(self):
        self._registry = {}

    def register(self, request_type: str, handler: PayloadHandler):
        self._registry[request_type] = handler

    def get(self, message_type: str) -> PayloadHandler:
        return self._registry.get(message_type)


class PulpoConfig(Config):

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

    @property
    def enable_banner(self: Config) -> bool:
        return self.getAsBool('banner.enable', "False")

    @property
    def banner_name(self: Config) -> bool:
        return self.get('banner.name', 'kessel')

    @property
    def banner_font(self: Config) -> bool:
        return self.get('banner.font', 'block')


class Pulpo():
    _queue_adapter = None
    _config = None
    _handler_registry = None

    def __init__(self, options: dict):
        self._config = PulpoConfig(options)

        self.log('init queue adapter')
        if self.config.queue_adapter_type == 'FileQueueAdapter':
            self._queue_adapter = FileQueueAdapter(self.config.get('file_queue_adapter'))
        else:
            raise Exception('invalid queue adapter type')

        self._handler_registry = HandlerRegistry()
        self.handler_registry.register('echo', EchoHandler(self.config.get('echo_handler')))
        self.handler_registry.register('lower', LowerCaseHandler(self.config.get('lower_handler')))
        self.handler_registry.register('upper', UpperCaseHandler(self.config.get('upper_handler')))

    @property
    def config(self) -> PulpoConfig:
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
        self.print_banner()
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
                    handler.handle(payload=message.body)

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

    def print_banner(self):
        print(f'print_banner {self.config.enable_banner=}')
        if self.config.enable_banner:
            banner = text2art(self.config.banner_name, font=self.config.banner_font, chr_ignore=True)
            print(banner)
        else:
            self.log('starting kessel')

    def log(self, *argv):
        logger.log(*argv, flush=self.config.enable_output_buffering)
