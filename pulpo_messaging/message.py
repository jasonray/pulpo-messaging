from datetime import timedelta
import datetime
import typing


class Message():
    # - priority
    # - delay
    # - ttr
    # - payload

    _components = None

    def __init__(self, id=None, payload=None, headers=None, request_type=None, delay=None, components:dict =None):
        if components:
            print('load from components')
            self._components = components
            print(f'id={self.id}')
        else:
            self._components = {}

        self._header = {}

        if id:
            self.id=id
        if payload:
            self.set_payload_item(payload=payload)
        if headers:
            self.set_header_item(headers=headers)
        if request_type:
            self.request_type=request_type
        if delay:
            self.delay=delay

    def __str__(self):
        return str(self._components)
    
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
        return self.get("id")
    
    @id.setter
    def id(self, value):
        self.set("id", value)

    @property
    def header(self) -> dict:
        return self._header

    def get_header_item(self, key: str):
        fqk = f'header.{key}'
        self.get(fqk)

    def set_header_item(self, headers):
        if isinstance(headers, dict):
            for key in headers:
                self.attach_header(key=key, value=headers[key])
        else:
            self.attach_header(key=headers)

    def attach_header(self, key: str, value: str = None):
        fqk = f'header.{key}'
        self.set(fqk,value)

    @property
    def delay(self):
        self.get_header_item('delay')

    @delay.setter
    def delay(self, value):
        if isinstance(value, timedelta):
            delta_dt = datetime.datetime.now() + timedelta(seconds=15)
        else:
            delta_dt = value
        self.attach_header('delay', delta_dt)

    @property
    def request_type(self):
        return self.get("header.request_type")

    @request_type.setter
    def request_type(self, value):
        self.set("header.request_type", value)

    @property
    def payload(self):
        return self.get('payload')

    def get_payload_item(self, key: str):
        fqk = f'payload.{key}'
        return self.get(fqk)

    def set_payload_item(self, payload):
        if isinstance(payload, dict):
            for key in payload:
                self.attach_payload_item(key=key, value=payload[key])
        else:
            self.attach_payload_item(key='body', value=payload)

    def attach_payload_item(self, key: str, value: str = None):
        fqk = f'payload.{key}'
        self.set(fqk,value)

    @property
    def body(self):
        return self.get_payload_item("body")
