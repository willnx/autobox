# -*- coding: UTF-8 -*-
"""A suite of unit tests for the log_exporter/main.py logic"""
import builtins
import unittest
from unittest.mock import patch, MagicMock
import logging


from log_exporter import main


class TestStaticVars(unittest.TestCase):
    """A suite of test cases for the static variables"""
    def test_loop_interval(self):
        """LOOP_INTERVAL value has not changed"""
        found = main.LOOP_INTERVAL
        expected = 10 # seconds

        self.assertEqual(found, expected)

    def test_cipher_key_file(self):
        """The default location of the CIPHER_KEY_FILE has not changed"""
        found = main.CIPHER_KEY_FILE
        expected = '/etc/vlab/log_sender.key'

        self.assertEqual(found, expected)


class TestGetLogger(unittest.TestCase):
    """A suite of test cases for the ``get_logger`` function"""

    def test_get_logger(self):
        """get_logger returns an instance of Pythons stdlib logging.Logger object"""
        logger = main.get_logger('single_logger')
        self.assertTrue(isinstance(logger, logging.Logger))

    def test_get_logger_one_handler(self):
        """
        We don't add a handler (causing spam multiple line outputs for a single msg)
        every time you call get_logger for the same logging object.
        """
        logger1 = main.get_logger('many_loggers')
        logger2 = main.get_logger('many_loggers')

        handlers = len(logger2.handlers)
        expected = 1

        self.assertEqual(handlers, expected)

class TestMain(unittest.TestCase):
    """A suite of test cases for the ``main`` function"""
    @patch.object(main, 'get_logger')
    @patch.object(main.docker, 'from_env')
    @patch.object(main.time, 'sleep')
    def test_main(self, fake_sleep, fake_from_env, fake_get_logger):
        """``main`` sleeps for less than LOOP_INTERVAL"""
        # only way to break the while True loop is raise an exception
        fake_sleep.side_effect = RuntimeError('breaking loop')
        with self.assertRaises(RuntimeError):
            main.main()

        call_args, _ = fake_sleep.call_args
        slept_for = call_args[0]

        self.assertTrue(main.LOOP_INTERVAL > slept_for)

    @patch.object(main.time, 'time')
    @patch.object(main, 'get_logger')
    @patch.object(main.docker, 'from_env')
    @patch.object(main.time, 'sleep')
    def test_main_slow_spawn(self, fake_sleep, fake_from_env, fake_get_logger, fake_time):
        """``main`` doesn't sleep if it took more than LOOP_INTERVAL to spawn/respawn workers"""
        fake_time.side_effect = [100, 200]
        fake_sleep.side_effect = RuntimeError('breaking loop')
        with self.assertRaises(RuntimeError):
            main.main()

        call_args, _ = fake_sleep.call_args
        slept_for = call_args[0]
        expected = 0

        self.assertEqual(slept_for, expected)

    @patch.object(main.time, 'time')
    @patch.object(main, 'get_logger')
    @patch.object(main.docker, 'from_env')
    @patch.object(main.time, 'sleep')
    def test_main_wonky_clock(self, fake_sleep, fake_from_env, fake_get_logger, fake_time):
        """``main`` doesn't sleep for more than LOOP_INTERVAL, even if someone adjusts the system clock"""
        fake_time.side_effect = [300, 200]
        fake_sleep.side_effect = RuntimeError('breaking loop')
        with self.assertRaises(RuntimeError):
            main.main()

        call_args, _ = fake_sleep.call_args
        slept_for = call_args[0]
        expected = 10

        self.assertEqual(slept_for, expected)


class TestGetTopic(unittest.TestCase):
    """A suite of test cases for the``get_topic`` function"""
    def test_get_api(self):
        """``get_topic`` successfully identifies web-server containers"""
        output = main.get_topic('vlab_insightiq-api_7')
        expected = 'web'

        self.assertEqual(output, expected)

    def test_get_worker(self):
        """``get_topic`` successfully identifies worker logs"""
        output = main.get_topic('vlab_insightiq-worker_36')
        expected = 'worker'

        self.assertEqual(output, expected)

    def test_get_ntp(self):
        """``get_topic`` successfully identifies NTP logs"""
        output = main.get_topic('vlab_ntp_1')
        expected = 'ntp'

        self.assertEqual(output, expected)

    def test_get_dns(self):
        """``get_topic`` successfully identifies DNS logs"""
        output = main.get_topic('vlab_dns_1')
        expected = 'dns'

        self.assertEqual(output, expected)

    def test_get_other(self):
        """``get_topic`` defaults to 'other' for non-specific/unknown log formats"""
        output = main.get_topic('vlab_foo_9001')
        expected = 'other'

        self.assertEqual(output, expected)


class TestSpawnWorkers(unittest.TestCase):
    """A suite of test cases for the ``spawn_workers`` function"""
    @classmethod
    def setUp(cls):
        fake_container1 = MagicMock()
        fake_container1.name = 'vlab_onefs-api_1'
        fake_container2 = MagicMock()
        fake_container2.name = 'vlab_dns_1'
        cls.fake_client = MagicMock()
        cls.fake_client.containers.list.return_value = [fake_container1, fake_container2]

    @patch.object(main, 'Exporter')
    def test_spawn_workers(self, fake_Exporter):
        """``spawn_workers`` creates an exporter thread for all new containers"""
        kafka_server = '127.0.0.0:9092'
        log = MagicMock()
        workers = {}

        output = main.spawn_workers(self.fake_client, workers, kafka_server, log).keys()
        expected = ['vlab_onefs-api_1', 'vlab_dns_1']
        # set() avoids false postivies due to order
        self.assertEqual(set(output), set(expected))

    @patch.object(main, 'Exporter')
    def test_no_spawn_workers(self, fake_Exporter):
        """``spawn_workers`` doesn't create a duplicate exporter for active containers"""
        kafka_server = '127.0.0.0:9092'
        log = MagicMock()
        workers = {'vlab_onefs-api_1' : MagicMock(), 'vlab_dns_1': MagicMock()}

        main.spawn_workers(self.fake_client, workers, kafka_server, log)

        self.assertFalse(fake_Exporter.called)


class TestRespawnWorkers(unittest.TestCase):
    """A suite of test cases for the ``respawn_workers`` function"""
    @classmethod
    def setUp(cls):
        fake_container1 = MagicMock()
        fake_container1.name = 'vlab_onefs-api_1'
        fake_container2 = MagicMock()
        fake_container2.name = 'vlab_dns_1'
        cls.fake_client = MagicMock()
        cls.fake_client.containers.list.return_value = [fake_container1, fake_container2]

    @patch.object(main, 'Exporter')
    def test_respawn_workers(self, fake_Exporter):
        """``respawn_workers`` only replaces not-alive Exporter threads"""
        fake_exporter1 = MagicMock()
        fake_exporter1.is_alive.return_value = True
        fake_exporter2 = MagicMock()
        fake_exporter2.is_alive.return_value = False
        workers = {'vlab_dns_1' : fake_exporter1, 'vlab_onefs-api_1' : fake_exporter2}
        kafka_server = '127.0.0.0:9092'
        log = MagicMock()

        main.respawn_workers(self.fake_client, workers, kafka_server, log)

        respawn_count = fake_Exporter.call_count
        expected = 1

        self.assertEqual(respawn_count, expected)

    @patch.object(main, 'Exporter')
    def test_respawn_workers_dangler(self, fake_Exporter):
        """``respawn_workers`` logs when a dead exporter has no newly deployed Docker container"""
        fake_exporter1 = MagicMock()
        fake_exporter1.is_alive.return_value = True
        fake_exporter2 = MagicMock()
        fake_exporter2.is_alive.return_value = False
        workers = {'vlab_dns_1' : fake_exporter1, 'vlab_cee-api_1' : fake_exporter2}
        kafka_server = '127.0.0.0:9092'
        log = MagicMock()

        main.respawn_workers(self.fake_client, workers, kafka_server, log)

        self.assertTrue(log.info.called)


class TestExporter(unittest.TestCase):
    """A suite of test cases for the ``Exporter`` object"""
    @patch.object(builtins, "open")
    @patch.object(main, 'KafkaProducer')
    @patch.object(main, 'Fernet')
    def test_exporter_init(self, fake_Fernet, fake_KafkaProducer, fake_open):
        """``Exporter`` sets up the cipher and Kafka connection upon init"""
        container = 'someContainer'
        topic = 'other'
        server = 'myKafka.org:9092'
        log = MagicMock()

        exporter = main.Exporter(container, topic, server, log)

        self.assertTrue(fake_KafkaProducer.called)
        self.assertTrue(fake_Fernet.called)

    @patch.object(builtins, "open")
    @patch.object(main, 'KafkaProducer')
    @patch.object(main, 'Fernet')
    def test_exporter_run(self, fake_Fernet, fake_KafkaProducer, fake_open):
        """``Exporter`` closes the connection to Kafka upon termination"""
        container = MagicMock()
        container.logs.return_value = [b'some log message\n']
        topic = 'other'
        server = 'myKafka.org:9092'
        log = MagicMock()

        exporter = main.Exporter(container, topic, server, log)
        exporter.run()

        self.assertTrue(exporter.conn.close.called)

    @patch.object(builtins, "open")
    @patch.object(main, 'KafkaProducer')
    @patch.object(main, 'Fernet')
    def test_exporter_grouping(self, fake_Fernet, fake_KafkaProducer, fake_open):
        """``Exporter`` Assumes a new line that begins with a space is part of the last message"""
        container = MagicMock()
        container.logs.return_value = [b'some log message\n', b' some more info\n', b'new stuff']
        topic = 'other'
        server = 'myKafka.org:9092'
        log = MagicMock()

        exporter = main.Exporter(container, topic, server, log)
        exporter.cipher.encrypt = lambda x: x
        exporter.run()

        the_args = exporter.conn.send.call_args
        sent_msg = the_args[0][1]
        expected = b'{"name":null,"log":"some log message\\n some more info\\n"}'

        self.assertEqual(sent_msg, expected)

    @patch.object(builtins, "open")
    @patch.object(main, 'KafkaProducer')
    @patch.object(main, 'Fernet')
    def test_exporter_error(self, fake_Fernet, fake_KafkaProducer, fake_open):
        """``Exporter`` logs all exceptions before terminating"""
        container = MagicMock()
        container.logs.return_value = ['some log message'] # should be Bytes; causes Traceback
        topic = 'other'
        server = 'myKafka.org:9092'
        log = MagicMock()

        exporter = main.Exporter(container, topic, server, log)
        exporter.run()

        self.assertTrue(exporter.log.exception.called)


if __name__ == "__main__":
    unittest.main()
