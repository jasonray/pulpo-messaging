import time
import signal
import os
import argparse
import datetime
import subprocess
import shlex


def main():
    # Initialize parser
    parser = argparse.ArgumentParser()

    # parser.add_argument("-t", "--ttl", dest="ttl", default=0, help="Specifies the amount of time (in seconds) to keep ghost alive")
    # parser.add_argument("-s", "--sleep", dest="sleep_duration", default=1, help="How often to check for closing process")
    # args = parser.parse_args()

    peekaboo = Peekaboo()
    peekaboo.start()


class Peekaboo:
    _shutdown_requested = False

    def __init__(self):
        signal.signal(signal.SIGABRT, self.signal_handler)

    @property
    def pid(self):
        return os.getpid()

    @property
    def sleep_duration(self):
        return 5

    @property
    def shutdown_requested(self):
        return self._shutdown_requested

    @shutdown_requested.setter
    def shutdown_requested(self, value):
        self._shutdown_requested = bool(value)

    def start(self):
        self.log("starting peekaboo")
        processes = [{'name': 'Process-A', 'command': 'python3 ./src/ghost.py -t 10 -s 2'}, {'name': 'Process-B', 'command': 'python3 ./src/ghost.py -t 120 -s 60'}, {'name': 'Process-C', 'command': 'python3 ./src/ghost.py -t 0 -s 60'}]

        continue_process = True
        self.start_time = time.time()
        self.log("start time", self.start_time)
        while continue_process:
            processes_still_alive = False
            for process in processes:
                # self.log(f'check on process [process={process["name"]}]')
                if process.get('process-handle'):
                    processes_still_alive = True
                    if self.shutdown_requested:
                        process.get('process-handle').poll()
                        self.log(f'check on process [process={process["name"]}][start_count={process["start_count"]}][pid={process.get("process-handle").pid}][returncode={process.get("process-handle").returncode}]')
                        if not process.get('process-handle').returncode == None:
                            self.log(f'process has terminate [process={process["name"]}]')
                            process['process-handle'] = None
                    else:
                        self.log(f'send signal to child process [process={process["name"]}]')
                        process.get('process-handle').send_signal(6)
                else:
                    if self.shutdown_requested:
                        self.log(f'peekabook shutting down, do not start process [process={process["name"]}]')
                    else:
                        command = process.get('command')
                        self.log(f'start process [process={process["name"]}][command={command}]')
                        self.log('command:', command)
                        p = subprocess.Popen(shlex.split(command))
                        process['process-handle'] = p
                        self.log(f'started process [process={process["name"]}][pid={p.pid}]')
                        if not process.get('start_count'):
                            process['start_count'] = 0
                        process['start_count'] += 1

            if (processes_still_alive) or not self.shutdown_requested:
                self.log('sleep..')
                time.sleep(self.sleep_duration)
            else:
                continue_process = False
        self.log(f"peekaboo shutdown")

    # even though the next method does not use frame, it must have it as part of the signature
    # pylint: disable-next=unused-argument
    def signal_handler(self, signum, frame):
        self.log(f'received signal [{signum}]')
        if signum == 6:
            self.shutdown_requested = True
            self.log('shutdown requested')

    def log(self, *messages):
        message_buffer = []
        for message_entry in messages:
            message_buffer.append(str(message_entry))
        message = ' '.join(message_buffer)
        timestamp = datetime.datetime.now()
        print(f'{timestamp}\t{self.pid}\t{message}')


if __name__ == "__main__":
    main()
