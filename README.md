# peekaboo
light weight process monitor

Concept:
from config file, read in a list of processes to monitor.
If process is not running, then it launches the process and tracks pid.
Every x seconds it will check if that process is still running.
