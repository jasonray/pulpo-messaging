import datetime
import uuid
import os

_kessel_root_directory = '/tmp/pulpo/unit-test'
_timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


def get_unique_base_path(tag: str = None):
    path = _kessel_root_directory
    path = os.path.join(path, _timestamp)
    if tag:
        path = os.path.join(path, tag)

    path = os.path.join(path, str(uuid.uuid4()))
    return path