import datetime
import json
import greenstalk
from statman import Statman
from greenstalk import Client as BeanstalkClient
from loguru import logger
from pulpo_config import Config
from pulpo_messaging.message import Message
from pulpo_messaging.queue_adapter import QueueAdapter

# from greenstalk import (
#     DEFAULT_PRIORITY,
#     DEFAULT_TTR,
#     DEFAULT_TUBE,
#     Address,
#     BuriedError,
#     Client,
#     DeadlineSoonError,
#     DrainingError,
#     Job,
#     JobTooBigError,
#     NotFoundError,
#     NotIgnoredError,
#     TimedOutError,
#     UnknownResponseError,
#     _parse_chunk,
#     _parse_response,
# )

# https://greenstalk.readthedocs.io/en/stable/index.html

# To start beanstalkd now and restart at login:
#   brew services start beanstalkd
# Or, if you don't want/need a background service you can just run:
#   /opt/homebrew/opt/beanstalkd/bin/beanstalkd -l 127.0.0.1 -p 11300


class BeanstalkdQueueAdapterConfig(Config):

    def __init__(self, options: dict = None, json_file_path: str = None):
        super().__init__(options=options, json_file_path=json_file_path)

    @property
    def host(self: Config) -> str:
        return self.get('host', '127.0.0.1')

    @property
    def port(self: Config) -> int:
        return self.get('port', 11300)

    # @property
    # def socket_timeout(self: Config) -> int:
    #     return self.get('socket_timeout', None)

    @property
    def default_tube(self: Config) -> int:
        return self.get('default_tube', "pulpo-beanstalk-queue-adapter")

    @property
    def encoding(self: Config) -> str:
        return self.get('encoding', "utf-8")

    @property
    def reserve_timeout(self: Config) -> int:
        return self.getAsInt('reserve_timeout', 0)

    @property
    def max_number_of_attempts(self: Config) -> bool:
        return self.getAsInt('max_number_of_attempts', 0)


class BeanstalkdQueueAdapter(QueueAdapter):

    _config = None
    _client = None

    def __init__(self, options: dict):
        super().__init__()
        logger.trace('BeanstalkdQueueAdapter init')

        self._config = BeanstalkdQueueAdapterConfig(options)
        address = (self.config.host, self.config.port)
        self._client = BeanstalkClient(address=address, encoding=self.config.encoding, watch=self.config.default_tube, use=self.config.default_tube)

        Statman.external_source('beanstalk', self.beanstalk_stat)

    @property
    def config(self) -> BeanstalkdQueueAdapterConfig:
        return self._config

    @property
    def client(self) -> BeanstalkClient:
        return self._client

    def enqueue(self, message: Message) -> Message:
        serialized_message = json.dumps(message._components, indent=2, default=str)
        # self.client.use( self.config.default_tube )
        put_job_id = self.client.put(body=serialized_message, delay=message.delayInSeconds)
        message.id = put_job_id
        logger.debug(f'enqueued message {message.id=}')
        return message

    def dequeue(self) -> Message:
        self.client.watch(self.config.default_tube)
        m = None
        try:
            while not m:
                logger.trace(f'BeanstalkdQueueAdapter dequeue begin reserve {self.config.reserve_timeout=}')
                job = self.client.reserve(timeout=self.config.reserve_timeout)
                logger.trace(f'BeanstalkdQueueAdapter dequeue reserve complete {job.id=}')
                m = self._load_message_from_job(job)

                if m and self.config.max_number_of_attempts:
                    self._get_message_attempts(m)
                    if m.attempts >= self.config.max_number_of_attempts:
                        logger.warning(f'message exceed max attempts {m.id=} {self.config.max_number_of_attempts=} {m.attempts=}')
                        self.commit(message=m, is_success=False)
                        m = None
                if m and m.expiration and m.expiration < datetime.datetime.now():
                    logger.warning(f'message expired {m.expiration=}')
                    self.commit(message=m, is_success=False)
                    m = None

        except greenstalk.TimedOutError:
            logger.trace('BeanstalkdQueueAdapter dequeue reserve timeout')
            # no message available
            m = None

        if m:
            logger.debug(f'dequeued message: {m.id=}')

        return m

    def _load_message_from_job(self, job):
        message_components = json.loads(job.body)
        m = Message(components=message_components)
        m.id = job.id
        return m

    def peek(self, message_id: str) -> Message:
        logger.debug(f'peek {message_id=}')
        job = self.client.peek(id=int(message_id))
        message = self._load_message_from_job(job)
        return message

    def _get_message_attempts(self, message: Message) -> Message:
        job_stats = self.client.stats_job(int(message.id))
        message.attempts = job_stats.get('releases')
        return message

    def delete(self, message_id: str, is_success: bool = True) -> Message:  # pylint: disable=unused-argument
        logger.trace(f'delete {message_id=}')
        self.client.delete(job=greenstalk.Job(id=int(message_id), body=''))

    def commit(self, message: Message, is_success: bool = True) -> Message:
        logger.trace(f'commit (delete) {message.id=}')
        self.client.delete(job=greenstalk.Job(int(message.id), message.body))

    def rollback(self, message: Message) -> Message:
        logger.trace(f'rollback (release) {message.id=}')
        self.client.release(job=greenstalk.Job(int(message.id), message.body))

    def beanstalk_stat(self, tube: str = None) -> Message:
        if not tube:
            tube = self.config.default_tube
        return self.client.stats_tube(tube)
