import os
import uuid

from kessel.payload_handler import PayloadHandler


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


class LowerCaseHandler(EchoHandler):

    def handle(self, payload: str):
        print('LowerCaseHandler.handle')
        EchoHandler.handle(self, payload.lower())


class UpperCaseHandler(EchoHandler):

    def handle(self, payload: str):
        print('UpperCaseHandler.handle')
        EchoHandler.handle(self, payload.upper())
