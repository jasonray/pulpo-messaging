import os
import uuid
import random
from .payload_handler import PayloadHandler
from .payload_handler import RequestResult


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
        return RequestResult.success_factory()

    @property
    def destination_directory(self) -> str:
        return self.config.get('destination_directory', '/tmp/kessel/EchoHandler-output')


class LowerCaseHandler(EchoHandler):

    def handle(self, payload: str):
        print('LowerCaseHandler.handle')
        return EchoHandler.handle(self, payload.lower())


class UpperCaseHandler(EchoHandler):

    def handle(self, payload: str):
        print('UpperCaseHandler.handle')
        return EchoHandler.handle(self, payload.upper())


class AlwaysSucceedHandler(EchoHandler):

    def handle(self, payload: str):
        print('AlwaysSucceedHandler.handle')
        result = RequestResult.success_factory()
        print(f'AlwaysSucceedHandler {result=}')
        return result


class AlwaysFailHandler(EchoHandler):

    def handle(self, payload: str):
        print('AlwaysFailHandler.handle')
        result = RequestResult.fatal_factory(error='something unexpected occurred')
        return result


class AlwaysTransientFailureHandler(EchoHandler):

    def handle(self, payload: str):
        print('AlwaysTransientFailureHandler.handle')
        result = RequestResult.transient_factory(error='something occurred, but if you try again in a moment it may succeed')
        return result


class FiftyFiftyHandler(EchoHandler):

    def handle(self, payload: str):
        print('FiftyFiftyHandler.handle')
        if random.randint(0, 1):
            result = RequestResult.success_factory()
        else:
            result = RequestResult.transient_factory(error='FiftyFiftyHandler failed, try again')
        return result
