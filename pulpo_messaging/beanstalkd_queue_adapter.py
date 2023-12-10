import os
import uuid
import time
import json
import random
import datetime
import greenstalk
from greenstalk import Client as BeanstalkClient
from statman import Statman
from pulpo_config import Config
from pulpo_messaging import logger
from pulpo_messaging.message import Message
from pulpo_messaging.queue_adapter import QueueAdapter

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


class BeanstalkdQueueAdapter(QueueAdapter):

    _config = None
    _client = None

    def __init__(self, options: dict):
        super().__init__()
        self.log('BeanstalkdQueueAdapter init')

        self._config = BeanstalkdQueueAdapterConfig(options)
        address = (self.config.host, self.config.port)
        self._client = BeanstalkClient(address= address,  encoding=self.config.encoding, watch=self.config.default_tube, use=self.config.default_tube)
        
        # print(f'stats_tube: {self.client.stats_tube( self.config.default_tube)}'  ) 



    @property
    def config(self) -> BeanstalkdQueueAdapterConfig:
        return self._config

    @property
    def client(self) -> BeanstalkClient:
        return self._client

    def enqueue(self, message: Message) -> Message:
        serialized_message = json.dumps(message._components, indent=2, default=str)        
        print(f'enqueue {serialized_message=}')
        # self.client.use( self.config.default_tube )
        put_job_id = self.client.put ( body= serialized_message )
        self.log(f'put message [{put_job_id=}]')
        message.id = put_job_id
        return message

    def dequeue(self) -> Message:
        self.client.watch( self.config.default_tube)
        try:
            self.log('BeanstalkdQueueAdapter dequeue begin reserve')
            job = self.client.reserve(timeout=1)
        except greenstalk.TimedOutError:
            self.log('BeanstalkdQueueAdapter dequeue reserve timeout')
            # no message available
            return None
        self.log(f'BeanstalkdQueueAdapter dequeue reserve complete {job.id=}')
        message_components = json.loads(job.body)
        print(f'{message_components=}')
        message = Message(components=message_components)
        message.id = job.id
        return message


    def commit(self, message: Message, is_success: bool = True) -> Message:
        self.client.delete( job=greenstalk.Job(message.id, message.body) )

    def rollback(self, message: Message) -> Message:        
        self.client.release( job=greenstalk.Job(message.id, message.body) )

    def log(self, *argv):
        logger.log(*argv, flush=True)
