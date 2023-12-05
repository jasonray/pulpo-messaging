from pulpo_config import Config


class PayloadHandler():
    _config = None

    def __init__(self, options: dict = None):
        self._config = Config(options=options)

    def handle(self, payload: str):
        pass

    @property
    def config(self) -> Config:
        return self._config
