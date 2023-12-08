from pulpo_config import Config


class RequestResult():
    _result = None
    _response_message_list = None
    _error = None

    RESULT_SUCCESS = 'success'
    RESULT_FAIL_FATAL = 'fatal'
    RESULT_FAIL_TRANSIENT = 'transient'

    def __init__(self, result: str, response_message_list=None, error=None):
        self._result = result
        self._response_message_list = []
        if response_message_list:
            for response_message in response_message_list:
                self._response_message_list.append(response_message)
        self._error = error

    @property
    def result(self):
        return self._result

    @property
    def response_messages(self) -> dict:
        return self._response_message_list

    @property
    def error(self):
        return self._error

    @property
    def isSuccess(self):
        return self.result == self.RESULT_SUCCESS

    @property
    def isFatal(self):
        return self.result == self.RESULT_FAIL_FATAL

    @property
    def isTransient(self):
        return self.result == self.RESULT_FAIL_TRANSIENT

    @staticmethod
    def success_factory(response_message_list=None) -> "RequestResult":
        return RequestResult(result=RequestResult.RESULT_SUCCESS, response_message_list=response_message_list)

    @staticmethod
    def fatal_factory(error=None, response_message_list=None) -> "RequestResult":
        return RequestResult(result=RequestResult.RESULT_FAIL_FATAL, error=error, response_message_list=response_message_list)

    @staticmethod
    def transient_factory(error=None, response_message_list=None) -> "RequestResult":
        return RequestResult(result=RequestResult.RESULT_FAIL_TRANSIENT, error=error, response_message_list=response_message_list)


class PayloadHandler():
    _config = None

    def __init__(self, options: dict = None):
        self._config = Config(options=options)

    def handle(self, payload: str) -> RequestResult:
        pass

    @property
    def config(self) -> Config:
        return self._config
