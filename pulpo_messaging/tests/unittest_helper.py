import datetime
import re
import uuid
import os

_pulpo_root_directory = '/tmp/pulpo/unit-test'
_timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


def get_unique_base_path(tag: str = None):
    path = _pulpo_root_directory
    path = os.path.join(path, _timestamp)
    if tag:
        path = os.path.join(path, tag)

    path = os.path.join(path, str(uuid.uuid4()))
    return path


def get_message_id_from_output(result):
    # print(f'{result=}')
    output = result.stdout
    output_str = output.decode('utf-8')
    # print(f'attempt to extract message id from string: {output_str}')
    match = re.search(r"(?:message\.id|message_id)='([^']+)'", output_str)

    if match:
        message_id = match.group(1)
    else:
        message_id = None

    return message_id
