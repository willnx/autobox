# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``collect_usage_stats`` module"""
import unittest
from unittest.mock import patch, MagicMock

from stat_collector import collect_usage_stats


class TestSpawnCollector(unittest.TestCase):
    """A suite of test cases for the ``spawn_collector`` function"""
    @patch.object(collect_usage_stats, 'UserCollector')
    def test_spawn_collector_starts(self, fake_UserCollector):
        """``spawn_collector`` starts the thread after creating it"""
        fake_vcenter = MagicMock()
        username = 'someuser'
        fake_influx = MagicMock()
        fake_collector = MagicMock()
        fake_UserCollector.return_value = fake_collector

        collect_usage_stats.spawn_collector(fake_vcenter, username, fake_influx)

        self.assertTrue(fake_collector.start.called)

    @patch.object(collect_usage_stats, 'UserCollector')
    def test_spawn(self, fake_UserCollector):
        """``spawn_collector`` returns the collector after creating it"""
        fake_vcenter = MagicMock()
        username = 'someuser'
        fake_influx = MagicMock()
        fake_collector = MagicMock()
        fake_UserCollector.return_value = fake_collector

        collector = collect_usage_stats.spawn_collector(fake_vcenter, username, fake_influx)

        self.assertTrue(collector is fake_collector)


class TestLookupUsers(unittest.TestCase):
    """A suite of test cases for the ``lookup_users`` function"""
    def test_lookup_users(self):
        """``lookup_users`` returns a set()``"""
        fake_folder = MagicMock()
        fake_folder.name = 'someuser'
        fake_entity = MagicMock()
        fake_entity.childEntity = [fake_folder]
        fake_vcenter = MagicMock()
        fake_vcenter.get_by_name.return_value = fake_entity

        users = collect_usage_stats.lookup_users(fake_vcenter)
        expected = {'someuser'}

        self.assertTrue(users, expected)


class TestDoWork(unittest.TestCase):
    """A suite of test cases for the ``do_work`` function"""
    @patch.object(collect_usage_stats, 'UserCollector')
    def test_do_work(self, fake_UserCollector):
        """``do_work`` returns all active UsageCollector after creating/deleting/updating them"""
        fake_collector = MagicMock()
        fake_UserCollector.return_value = fake_collector
        fake_folder = MagicMock()
        fake_folder.name = 'someuser'
        fake_entity = MagicMock()
        fake_entity.childEntity = [fake_folder]
        fake_vcenter = MagicMock()
        fake_vcenter.get_by_name.return_value = fake_entity
        fake_influx = MagicMock()
        fake_user_collectors = {}
        fake_log = MagicMock()

        user_collectors = collect_usage_stats.do_work(fake_vcenter, fake_influx, fake_user_collectors, fake_log)
        expected = {'someuser' : fake_collector}

        self.assertEqual(user_collectors, expected)

    @patch.object(collect_usage_stats, 'UserCollector')
    def test_do_work_deleted(self, fake_UserCollector):
        """``do_work`` deletes a collector if that user is no longer part of vLab"""
        fake_collector = MagicMock()
        fake_collector2 = MagicMock()
        fake_UserCollector.return_value = fake_collector
        fake_folder = MagicMock()
        fake_folder.name = 'someuser'
        fake_entity = MagicMock()
        fake_entity.childEntity = [fake_folder]
        fake_vcenter = MagicMock()
        fake_vcenter.get_by_name.return_value = fake_entity
        fake_influx = MagicMock()
        fake_user_collectors = {'deletedUser' : fake_collector2}
        fake_log = MagicMock()

        user_collectors = collect_usage_stats.do_work(fake_vcenter, fake_influx, fake_user_collectors, fake_log)
        expected = {'someuser' : fake_collector}

        self.assertEqual(user_collectors, expected)

    @patch.object(collect_usage_stats, 'UserCollector')
    def test_do_work_respawn(self, fake_UserCollector):
        """``do_work`` Respawns a collector if it's dead for whatever reason"""
        fake_collector = MagicMock()
        fake_collector.is_alive = False
        fake_UserCollector.return_value = fake_collector
        fake_folder = MagicMock()
        fake_folder.name = 'someuser'
        fake_entity = MagicMock()
        fake_entity.childEntity = [fake_folder]
        fake_vcenter = MagicMock()
        fake_vcenter.get_by_name.return_value = fake_entity
        fake_influx = MagicMock()
        fake_user_collectors = {}
        fake_log = MagicMock()

        user_collectors = collect_usage_stats.do_work(fake_vcenter, fake_influx, fake_user_collectors, fake_log)
        expected = {'someuser' : fake_collector}

        self.assertEqual(user_collectors, expected)


class TestMain(unittest.TestCase):
    """A suite of test cases for the ``main`` function"""
    @patch.object(collect_usage_stats, 'do_work')
    @patch.object(collect_usage_stats.time, 'time')
    @patch.object(collect_usage_stats.time, 'sleep')
    @patch.object(collect_usage_stats, 'InfluxDB')
    @patch.object(collect_usage_stats, 'vCenter')
    @patch.object(collect_usage_stats, 'get_logger')
    def test_main(self, fake_get_logger, fake_vCenter, fake_InfluxDB, fake_sleep, fake_time, fake_do_work):
        """``collect_usage_stats.main`` pauses for a set amount of time between loops"""
        influx_server = 'some.influx.org'
        influx_user = 'alice'
        influx_password =  'iLoveCats'
        vcenter_server = 'some.vcenter.org'
        vcenter_user = 'bob'
        vcenter_password = 'IloveCats'
        fake_log = MagicMock()
        fake_get_logger.return_value = fake_log
        fake_sleep.side_effect = [RuntimeError('Breaking while True loop')]
        fake_time.side_effect = [10, 20]

        try:
            collect_usage_stats.main(influx_server, influx_user, influx_password, vcenter_server, vcenter_user, vcenter_password)
        except RuntimeError:
            # artifically causing this error to break 'while True' loop in 'main'
            pass
        the_args, _ = fake_sleep.call_args
        slept_for = the_args[0]
        expected_sleep = collect_usage_stats.CHECK_INTERVAL - 10 # delta between calls to time()

        self.assertEqual(slept_for, expected_sleep)

if __name__ == '__main__':
    unittest.main()
