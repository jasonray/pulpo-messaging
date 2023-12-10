import os
import uuid
import time
import json
import random
import datetime
from pystalk import BeanstalkClient
from statman import Statman
from pulpo_config import Config
from pulpo_messaging import logger
from pulpo_messaging.message import Message
from pulpo_messaging.queue_adapter import QueueAdapter


class BeanstalkdQueueAdapterConfig(Config):

    def __init__(self, options: dict = None, json_file_path: str = None):
        super().__init__(options=options, json_file_path=json_file_path)

    @property
    def host(self: Config) -> str:
        return self.get('host', '127.0.0.1')

    @property
    def port(self: Config) -> int:
        return self.get('port', 11300)

    @property
    def socket_timeout(self: Config) -> int:
        return self.get('socket_timeout', None)

    @property
    def default_tube(self: Config) -> int:
        return self.get('default_tube', "pulpo-beanstalk-queue-adapter")


class BeanstalkdQueueAdapter(QueueAdapter):

    _config = None
    _client = None

    def __init__(self, options: dict):
        super().__init__()
        self.log('BeanstalkdQueueAdapter init')

        self._config = BeanstalkdQueueAdapterConfig(options)
        self._client = BeanstalkClient(host= self.config.host, port=self.config.port, socket_timeout=self.config.socket_timeout)
        
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
        self.client.use( self.config.default_tube )
        result, put_job_id = self.client.put_job( data= serialized_message )
        self.log(f'put message [{result=}][{put_job_id=}]')
        message.id = put_job_id
        return message

    def dequeue(self) -> Message:
        self.client.watch( self.config.default_tube)
        job = self.client.reserve_job(timeout=1)
        print(f'{job.job_id=} {job=}')
        print(f'{job.job_data=}')
        message_components = json.loads(job.job_data)
        print(f'{message_components=}')
        message = Message(components=message_components)
        message.id = job.job_id
        print(f'{message.body=}')
        return message


    def commit(self, message: Message, is_success: bool = True) -> Message:
        pass

    def rollback(self, message: Message) -> Message:
        pass

    def log(self, *argv):
        logger.log(*argv, flush=True)
