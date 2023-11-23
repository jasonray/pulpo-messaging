import datetime
import uuid
import os
import unittest
from kessel.kessel import FileQueueAdapter
from kessel.kessel import QueueAdapter
from kessel.kessel import Message
from kessel.kessel import Kessel
from statman import Statman
from statman import Stopwatch

class TestKessel(unittest.TestCase):

    def test_config_shutdown_after_number_of_empty_iterations(self):
        config = {}
        config['shutdown_after_number_of_empty_iterations'] = 10
        kessel = Kessel(config)
        self.assertEqual(kessel.shutdown_after_number_of_empty_iterations,10)
