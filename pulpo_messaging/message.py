class Message():
    # - priority
    # - delay
    # - ttr
    # - payload

    _id = None
    _payload = None
    _request_type = None
    _header = None

    def __init__(self, payload=None, headers=None, request_type=None):
        self._payload = payload
        self._header = {}
        self.attach_headers(headers)

        self._request_type = request_type

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

    @property
    def request_type(self):
        return self._request_type

    @property
    def payload(self):
        return self._payload

    def attach_headers(self, headers):
        if isinstance(headers, dict):
            for key in headers:
                self.attach_header(key=key, value=headers[key])
        else:
            self.attach_header(key=headers)


    def attach_header(self, key:str, value:str=None):
        self.header[key]=value