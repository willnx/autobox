# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``firewall.py`` worker module"""
import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import builtins

import ujson
from cryptography.fernet import Fernet, InvalidToken

from log_processor.processors import firewall
from log_processor.worker import Worker


def fake_get_cipher(self):
    self.cipher = Fernet(Fernet.generate_key())


class TestFirewallWorker(unittest.TestCase):
    """A suite of test cases for the ``FirewallWorker`` object"""

    @classmethod
    def setUpClass(cls):
        """Runs once for the entire test suite, before any test cases"""
        os.environ['INFLUXDB_SERVER'] = 'myInfluxServer'
        os.environ['INFLUXDB_USER'] = 'alice'
        os.environ['INFLUXDB_PASSWD_FILE'] = './.fake_file.txt'
        os.environ['CIPHER_KEY_FILE'] = './.fake_file.txt'

    @classmethod
    def tearDownClass(cls):
        """Run once for the entire test suite, after all test cases"""
        os.environ.pop('INFLUXDB_SERVER', None)
        os.environ.pop('INFLUXDB_USER', None)
        os.environ.pop('INFLUXDB_PASSWD_FILE', None)
        os.environ.pop('CIPHER_KEY_FILE', None)

    @classmethod
    def setUp(cls):
        """Runs before every test case"""
        cls.patcher = patch('log_processor.processors.firewall.InfluxDB')
        cls.fake_InfluxDB = cls.patcher.start()

    @classmethod
    def tearDown(cls):
        """Runs after every test case"""
        cls.patcher.stop()
        cls.fake_InfluxDB = None

    @patch.object(firewall.FirewallWorker, 'get_cipher', fake_get_cipher)
    @patch('builtins.open', new_callable=mock_open())
    def test_init_firewallworker(self, fake_open):
        """``FirewallWorker`` Is a sublcass of 'log_processor.worker.Worker'"""
        work_queue = MagicMock()
        idle_queue = MagicMock()
        fw = firewall.FirewallWorker(work_group='firewall', work_queue=work_queue, idle_queue=idle_queue)

        self.assertTrue(isinstance(fw, Worker))

    @patch.object(firewall.FirewallWorker, 'get_cipher', fake_get_cipher)
    @patch('builtins.open', new_callable=mock_open())
    def test_extract(self, fake_open):
        """``FirewallWorker`` can convert the encrypted JSON data into a usable object"""
        work_queue = MagicMock()
        idle_queue = MagicMock()
        fw = firewall.FirewallWorker(work_group='firewall', work_queue=work_queue, idle_queue=idle_queue)

        test_obj = {"works": True}
        test_data = fw.cipher.encrypt(ujson.dumps(test_obj).encode())
        answer = fw.extract(test_data)

        self.assertEqual(test_obj, answer)

    @patch.object(firewall.FirewallWorker, 'get_cipher', fake_get_cipher)
    @patch('builtins.open', new_callable=mock_open())
    def test_flush(self, fake_open):
        """``FirewallWorker`` tells the 'InfluxDB' to flush pending writes on termination"""
        work_queue = MagicMock()
        idle_queue = MagicMock()
        fw = firewall.FirewallWorker(work_group='firewall', work_queue=work_queue, idle_queue=idle_queue)
        fw.flush_on_term()

        called_flush = fw.influx.flush.called

        self.assertTrue(called_flush)

    @patch.object(firewall.FirewallWorker, 'get_cipher', fake_get_cipher)
    @patch('builtins.open', new_callable=mock_open())
    def test_process_data(self, fake_open):
        """``FirewallWorker`` writes data to InfluxDB while processing data"""
        work_queue = MagicMock()
        idle_queue = MagicMock()
        fw = firewall.FirewallWorker(work_group='firewall', work_queue=work_queue, idle_queue=idle_queue)

        test_obj = {'user' : 'bob', 'time' : 1234, 'source' : '192.168.1.56', 'target' : '10.7.1.2'}
        test_data = fw.cipher.encrypt(ujson.dumps(test_obj).encode())
        fw.process_data(test_data)

        called_write = fw.influx.write.called

        self.assertTrue(called_write)

    @patch.object(firewall.FirewallWorker, 'get_cipher', fake_get_cipher)
    @patch('builtins.open', new_callable=mock_open())
    def test_process_data_invalid_token(self, fake_open):
        """``FirewallWorker`` gracefully handles incorrectly encrypted data"""
        work_queue = MagicMock()
        idle_queue = MagicMock()
        fw = firewall.FirewallWorker(work_group='firewall', work_queue=work_queue, idle_queue=idle_queue)
        fw.log = MagicMock()

        cipher = Fernet(Fernet.generate_key())
        test_obj = {'user' : 'bob', 'time' : 1234, 'source' : '192.168.1.56', 'target' : '10.7.1.2'}
        test_data = cipher.encrypt(ujson.dumps(test_obj).encode())
        fw.process_data(test_data)

        called_write = fw.influx.write.called

        self.assertFalse(called_write)

    @patch.object(firewall.FirewallWorker, 'get_cipher', fake_get_cipher)
    @patch('builtins.open', new_callable=mock_open())
    def test_process_data_bad_json(self, fake_open):
        """``FirewallWorker`` gracefully handles invalid JSON data"""
        work_queue = MagicMock()
        idle_queue = MagicMock()
        fw = firewall.FirewallWorker(work_group='firewall', work_queue=work_queue, idle_queue=idle_queue)
        fw.log = MagicMock()

        cipher = Fernet(Fernet.generate_key())
        test_data = cipher.encrypt(b'Not JSON')
        fw.process_data(test_data)

        called_write = fw.influx.write.called

        self.assertFalse(called_write)


if __name__ == '__main__':
    unittest.main()
