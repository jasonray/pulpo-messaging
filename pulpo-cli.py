import argparse
import sys
from loguru import logger
from pulpo_messaging.message import Message
from pulpo_messaging.queue_adapter import QueueAdapter
from pulpo_messaging.kessel import Pulpo, PulpoConfig


def main():
    parser = argparse.ArgumentParser(prog='pulpo-cli', description='Provides a set of common pulpo-messaging utilities')
    parser.add_argument('command')
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true')
    parser.add_argument('--publish.payload', '--payload', dest='payload', help='when performing `publish`, payload portion of the body of the message', type=str)
    parser.add_argument('--publish.number', '--n',  dest='number_of_messages', help='when performing `publish`, number of messages to publish', type=int)
    parser.add_argument('--message_id', '--id', dest='message_id', help='message_id for peek, delete', type=str)
    parser.add_argument('--config', type=str, help='path to config file')

    parser.add_argument('--file_queue_adapter.base_path', type=str)

    # parser.add_argument('--server', '-s', dest='host', default='127.0.0.1', help='beanstalkd host/server', type=str)
    # parser.add_argument('--port', '-p', dest='port', default=11300, help='beanstalkd port', type=int)
    # parser.add_argument('--encoding', '-e', dest='encoding', default='utf-8', help='encoding', type=str)
    # parser.add_argument('--tube', '-t', dest='tube', default='default', help='beanstalkd tube', type=str)
    # parser.add_argument('--id', dest='job_id', help='job id (for peek)', type=int)
    # parser.add_argument('--put.priority', '--priority', dest='priority', default=5, help='when performing `put`, priority of message', type=int)
    # parser.add_argument('--put.delay', '--delay', dest='delay', default=0, help='when performing `put`, delay of message in seconds', type=int)
    # parser.add_argument('--put.ttr', '--ttr', dest='ttr', default=0, help='when performing `put`, ttr in seconds', type=int)

    args, unknown = parser.parse_known_args()

    if args.verbose:
        logger.remove()
        logger.add(sys.stdout, level="TRACE")
    else:
        logger.remove()
        logger.add(sys.stdout, format="{message}", level="SUCCESS")


    command_parts = args.command.split('.')
    command_parent = command_parts[0]

    match command_parent:
        case 'queue':
            QueueCommands.run(args)
        case _:
            raise Exception(f'invalid command [{args.command}][{command_parent}]')

    return 0


class QueueCommands():
    @staticmethod
    def run(args):
        command_parts = args.command.split('.')
        command_child = command_parts[1]

        config = PulpoConfig().fromJsonFile(file_path=args.config)
        file_queue_adapter_config = config.get('file_queue_adapter')
        config =PulpoConfig().fromJsonFile(file_path=args.config).fromArgumentParser(args)
        file_queue_adapter_config = config.get('file_queue_adapter')



        pulpo = Pulpo(PulpoConfig().fromJsonFile(file_path=args.config).fromArgumentParser(args))
        client = pulpo.initialize_queue_adapter()

        match command_child:
            case 'pop' | 'dequeue':
                QueueCommands.pop(client=client)
            case 'peek':
                QueueCommands.peek(client=client, message_id=args.message_id)
            case 'delete':
                QueueCommands.delete(client=client, message_id=args.message_id)
            case 'publish' | 'put' | 'enqueue':
                if args.number_of_messages:
                    n=args.number_of_messages
                else:
                    n=1
                for i in range(1,n+1):
                    QueueCommands.publish(client=client, body=args.payload)
            case _:
                raise Exception(f'invalid command [{command_child}]')


    @staticmethod
    def pop(client: QueueAdapter):
        message = client.dequeue()
        if message:
            client.commit (message)
            logger.success(f'pop: {message.id=} \n{message}')
        else:
            logger.warning(f'pop: no message available')


    @staticmethod
    def peek(client: QueueAdapter, message_id: str):
        if not message_id:
            raise Exception(f'invalid job id {message_id}')
        message = client.peek(message_id=message_id)
        if message:
            logger.success(f'peek: {message.id=} \n{message}')
        else:
            logger.warning(f'peek: no message available')

    @staticmethod
    def delete(client: QueueAdapter, message_id: str):
        if not message_id:
            raise Exception(f'invalid job id {message_id}')
        client.delete(message_id=message_id)
        logger.success(f'delete: {message_id=}')

    @staticmethod
    def publish(client: QueueAdapter, body: str):
        message = Message(payload = body)
        message.set_header_item("source", "pulpo-cli.py")
        message = client.enqueue(message)
        logger.success(f'put: {message.id=}')

    # @staticmethod
    # def delete(client: QueueAdapter, job_id: int):
    #     client.delete(job=job_id)
    #     logger.info(f'delete: {job_id=}')


if __name__ == '__main__':
    main()
