import signal
import time
from statman import Statman
from pulpo_config import Config
from art import text2art
from loguru import logger
from .file_queue_adapter import FileQueueAdapter
from .beanstalkd_queue_adapter import BeanstalkdQueueAdapter
from .message import Message
from .payload_handler import PayloadHandler, RequestResult
from .queue_adapter import QueueAdapter
from .sample_handlers import AlwaysFailHandler, AlwaysSucceedHandler, EchoHandler, FiftyFiftyHandler, LowerCaseHandler, UpperCaseHandler


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

    @property
    def enable_statman_reporting(self: Config) -> bool:
        return self.getAsBool('enable_statman_reporting', False)


class Pulpo():
    _queue_adapter = None
    _config = None
    _handler_registry = None
    _shutdown_requested = False

    def __init__(self, options: dict = None, queue_adapter=None):
        self._config = PulpoConfig(options)

        if queue_adapter:
            self._queue_adapter = queue_adapter

        self._handler_registry = HandlerRegistry()
        self.handler_registry.register('echo', EchoHandler(self.config.get('echo_handler')))
        self.handler_registry.register('lower', LowerCaseHandler(self.config.get('lower_handler')))
        self.handler_registry.register('upper', UpperCaseHandler(self.config.get('upper_handler')))
        self.handler_registry.register('success', AlwaysSucceedHandler(self.config.get('AlwaysSucceedHandler')))
        self.handler_registry.register('fail', AlwaysFailHandler(self.config.get('AlwaysFailHandler')))
        self.handler_registry.register('fail', FiftyFiftyHandler(self.config.get('FiftyFiftyHandler')))

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self._shutdown_requested = False

    def initialize_queue_adapter(self, queue_adapter: QueueAdapter = None):
        logger.debug('init queue adapter')
        if self._queue_adapter:
            pass  #queue adapter already initialized
        elif queue_adapter:
            if isinstance(queue_adapter, QueueAdapter):
                self._queue_adapter = queue_adapter
            else:
                raise Exception('invalid queue adapter')
        elif self.config.queue_adapter_type in {'FileQueueAdapter', 'file_queue_adapter'}:
            self._queue_adapter = FileQueueAdapter(self.config.get('file_queue_adapter'))
        elif self.config.queue_adapter_type in {'BeanstalkdQueueAdapter', 'beanstalkd_queue_adapter'}:
            self._queue_adapter = BeanstalkdQueueAdapter(self.config.get('beanstalkd_queue_adapter'))
        else:
            raise Exception(f'invalid queue adapter type {self.config.queue_adapter_type}')
        return self._queue_adapter

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
        if not self._queue_adapter:
            self.initialize_queue_adapter()

        logger.info(f'publish message [{message.request_type}]')
        return self.queue_adapter.enqueue(message)

    def start(self) -> Message:
        self.print_banner()
        self.initialize_queue_adapter()
        continue_processing = True
        iterations_with_no_messages = 0

        Statman.stopwatch('kessel.message_streak_tm', autostart=True)
        Statman.calculation(
            'kessel.message_streak_messages_per_s').calculation_function = lambda: Statman.gauge('kessel.message_streak_cnt').value / Statman.stopwatch('kessel.message_streak_tm').value
        while continue_processing and not self._shutdown_requested:
            Statman.gauge('kessel.dequeue-attempts').increment()
            message = self.queue_adapter.dequeue()

            if message:
                iterations_with_no_messages = 0
                self.handle_message(message)
            else:
                iterations_with_no_messages += 1
                logger.trace(f'no message available [iteration with no messages = {iterations_with_no_messages}][max = {self.config.shutdown_after_number_of_empty_iterations}]')

                Statman.stopwatch('kessel.message_streak_tm').stop()
                if self.config.enable_statman_reporting:
                    Statman.report(output_stdout=False, log_method=logger.debug)

                if iterations_with_no_messages >= self.config.shutdown_after_number_of_empty_iterations:
                    logger.info('no message available, shutdown')
                    continue_processing = False
                else:
                    logger.debug('no message available, sleep')
                    time.sleep(self.config.sleep_duration)
                    Statman.stopwatch('kessel.message_streak_tm').start()
                    Statman.gauge('kessel.message_streak_cnt').value = 0

        logger.info('pulpo-messaging shutdown')

    def handle_message(self, message) -> RequestResult:
        Statman.gauge('kessel.dequeue').increment()
        Statman.gauge('kessel.message_streak_cnt').increment()

        logger.info(f'processing message [id={message.id}][type={message.request_type}]')
        handler = self.handler_registry.get(message.request_type)
        logger.trace(f'handler: {handler}')
        if handler is None:
            logger.warning(f'WARNING no handler for message type {message.request_type}')
            result = RequestResult.fatal_factory(f'WARNING no handler for message type {message.request_type}')
        elif isinstance(handler, PayloadHandler):
            result = handler.handle(payload=message.payload)
        else:
            logger.warning(f'WARNING unexpected handler {message.request_type} {handler}')
            result = RequestResult.fatal_factory(f'WARNING unexpected handler {message.request_type} {handler}')
        logger.trace(f'processing complete: {result=}')

        if result.isSuccess:
            logger.info(f'message successfully processed [id={message.id}][type={message.request_type}]')
            self.queue_adapter.commit(message=message, is_success=True)
            Statman.gauge('kessel.messages.success').increment()
            Statman.gauge('kessel.commit').increment()
            logger.debug('commit complete')
        elif result.isTransient:
            logger.warning(f'message failed due to transient condition [id={message.id}][type={message.request_type}]')
            self.queue_adapter.rollback(message=message)
            Statman.gauge('kessel.messages.transient').increment()
            Statman.gauge('kessel.rollback').increment()
            logger.debug('rollback complete')
        elif result.isFatal:
            logger.warning(f'message failed due to fatal exception [id={message.id}][type={message.request_type}]')
            self.queue_adapter.commit(message=message, is_success=False)
            Statman.gauge('kessel.messages.fatal').increment()
            Statman.gauge('kessel.commit').increment()
            logger.debug('commit complete')
        else:
            pass

        Statman.gauge('kessel.messages_processed').increment()

        return RequestResult

    def print_banner(self):
        if self.config.enable_banner:
            banner = text2art(self.config.banner_name, font=self.config.banner_font, chr_ignore=True)
            print(banner)
        else:
            logger.debug('starting pulpo-messaging')

    def signal_handler(self, signum, frame):  # pylint: disable=unused-argument
        logger.info('Signal handler called with signal, requesting shutdown', signum)
        self._shutdown_requested = True
