from datetime import timedelta
import datetime
import typing


class Message():
    # - priority
    # - delay
    # - ttr
    # - payload

    _components = None

    def __init__(self, message_id=None, body: dict = None, payload=None, header: dict = None, request_type=None, delay=None, expiration: datetime.datetime = None, components: dict = None):
        self._components = {}

        if components:
            self._components = components
        if message_id:
            self.id = message_id
        if body:
            self.__store_body(body)
        if payload:
            self.set_body_item(key='payload', value=payload)
        if header:
            self.__store_header(header)
        if request_type:
            self.request_type = request_type
        if delay:
            self.delay = delay
        if expiration:
            self.expiration = expiration

    def __str__(self):
        return str(self._components)

    def __store_header(self, header):
        if isinstance(header, dict):
            for key in header:
                self.set_header_item(key=key, value=header[key])
        elif isinstance(header, set):
            for key in header:
                self.set_header_item(key=key, value=None)

    def __store_body(self, body):
        for key in body:
            self.set_body_item(key=key, value=body[key])

    def get(self, key: str):
        keys = key.split('.')

        value = self._components
        for subkey in keys:
            if value:
                if subkey in value:
                    value = value[subkey]
                else:
                    value = None
            else:
                value = None

        return value

    def getAsDate(self, key: str):
        value = self.get(key)
        if isinstance(value, str):
            return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
        return value

    def set(self, key: str, value: typing.Any):
        keys = key.split('.')

        parent = self._components
        for key_number in range(0, len(keys) - 1):
            key = keys[key_number]
            if not key in parent:
                parent[key] = {}
            parent = parent.get(key)

        last_key = keys[len(keys) - 1]
        parent[last_key] = value

    @property
    def id(self):
        return str(self.get("id"))

    @id.setter
    def id(self, value):
        self.set("id", value)

    @property
    def header(self) -> dict:
        return self.get('header')

    def get_header_item(self, key: str) -> str:
        fqk = f'header.{key}'
        return self.get(fqk)

    def set_header_item(self, key: str, value: str = None):
        fqk = f'header.{key}'
        self.set(fqk, value)

    @property
    def delay(self):
        fqk = 'header.delay'
        return self.getAsDate(fqk)

    @property
    def delayInSeconds(self):
        delay_dt = self.delay

        if not delay_dt:
            return 0
        now = datetime.datetime.now()
        delay_delta = delay_dt - now
        value = delay_delta.total_seconds()
        value = max(value, 0)
        value = round(value)
        return value

    @delay.setter
    def delay(self, value):
        if isinstance(value, timedelta):
            delta_dt = datetime.datetime.now() + value
        elif isinstance(value, int):
            delta = timedelta(seconds=value)
            delta_dt = datetime.datetime.now() + delta
        else:
            delta_dt = value
        fqk = 'header.delay'
        self.set(fqk, delta_dt)

    @property
    def request_type(self):
        return self.get_header_item('request_type')

    @request_type.setter
    def request_type(self, value):
        self.set_header_item('request_type', value)

    @property
    def expiration(self) -> datetime.datetime:
        value = self.get_header_item('expiration')
        if isinstance(value, str):
            value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')  # pylint: disable=redefined-variable-type
        return value

    @expiration.setter
    def expiration(self, value):
        #%Y-%m-%d %H:%M:%S
        if isinstance(value, str):
            value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        self.set_header_item('expiration', value)

    @property
    def attempts(self) -> int:
        header_item = self.get_header_item('attempts')
        if header_item is None:
            value = 0
        else:
            value = int(header_item)
        return value

    @attempts.setter
    def attempts(self, value: int):
        self.set_header_item('attempts', value)

    def get_body_item(self, key: str):
        fqk = f'body.{key}'
        return self.get(fqk)

    def set_body_item(self, key: str, value: str = None):
        fqk = f'body.{key}'
        self.set(fqk, value)

    @property
    def body(self) -> dict:
        return self.get('body')

    @property
    def payload(self):
        return self.get_body_item('payload')
