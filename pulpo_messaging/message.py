class Message():
    # - priority
    # - delay
    # - ttr
    # - payload

    _id = None
    _payload = None
    _header = None

    def __init__(self, payload=None, headers=None, request_type=None):
        self._payload = {}
        self._header = {}

        self.attach_payload(payload=payload)
        self.attach_headers(headers=headers)

        self.attach_header(key="request_type", value=request_type)

    def __str__(self):
        serialized = ''
        serialized += 'm1.0 \n'
        serialized += '' + self.payload
        return serialized

    @property
    def id(self):
        return self._id

    @property
    def header(self) -> dict:
        return self._header

    def header_item(self, key: str):
        return self.header.get(key)

    @property
    def request_type(self):
        return self.header.get("request_type")

    @property
    def payload(self):
        return self._payload

    @property
    def body(self):
        return self.payload.get("body")

    def payload_item(self, key: str):
        return self.payload.get(key)

    def attach_headers(self, headers):
        if isinstance(headers, dict):
            for key in headers:
                self.attach_header(key=key, value=headers[key])
        else:
            self.attach_header(key=headers)

    def attach_header(self, key: str, value: str = None):
        self.header[key] = value

    def attach_payload(self, payload):
        if isinstance(payload, dict):
            for key in payload:
                self.attach_payload_item(key=key, value=payload[key])
        else:
            self.attach_payload_item(key='body', value=payload)

    def attach_payload_item(self, key: str, value: str = None):
        self.payload[key] = value
