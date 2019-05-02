# -*- coding: UTF-8 -*-
"""A suite of test cases for the ``collect_inf_stats`` module"""
import unittest
from unittest.mock import patch, MagicMock

from stat_collector import collect_inf_stats


class TestImportantVMs(unittest.TestCase):
    """A suite of test cases for the ``important_vms`` function"""
    def test_important_vms(self):
        """``important_vms`` The set of VM names has not been changed"""
        vms = collect_inf_stats.important_vms()
        expected = set(['vlabServer', 'vCenter', 'vLabData', 'vLabAutoBox'])

        self.assertEqual(vms, expected)


class TestUnityStats(unittest.TestCase):
    """A suite of test cases for the ``unity_stats`` function"""
    def test_important_vms(self):
        """``unity_stats`` The set of SAN stat names has not been changed"""
        stats = collect_inf_stats.unity_stats()
        expected = set(['lun_latency', 'lun_io', 'net_bytes_in', 'net_bytes_out', 'ram_active'])

        self.assertEqual(stats, expected)


class TestUnityStatMap(unittest.TestCase):
    """A suite of test cases for the ``unity_stat_map`` function"""
    def test_lun_latency(self):
        """``unity_stat_map`` The stat name lun_latency returns the correct object"""
        stat = collect_inf_stats.unity_stat_map('lun_latency')

        self.assertTrue(stat is collect_inf_stats.UnityLunLatency)

    def test_lun_io(self):
        """``unity_stat_map`` The stat name lun_io returns the correct object"""
        stat = collect_inf_stats.unity_stat_map('lun_io')

        self.assertTrue(stat is collect_inf_stats.UnityLunIO)

    def test_net_bytes_in(self):
        """``unity_stat_map`` The stat name net_bytes_in returns the correct object"""
        stat = collect_inf_stats.unity_stat_map('net_bytes_in')

        self.assertTrue(stat is collect_inf_stats.UnityNetBytesIn)

    def test_net_bytes_out(self):
        """``unity_stat_map`` The stat name net_bytes_out returns the correct object"""
        stat = collect_inf_stats.unity_stat_map('net_bytes_out')

        self.assertTrue(stat is collect_inf_stats.UnityNetBytesOut)

    def test_ram_active(self):
        """``unity_stat_map`` The stat name ram_active returns the correct object"""
        stat = collect_inf_stats.unity_stat_map('ram_active')

        self.assertTrue(stat is collect_inf_stats.UnityMemoryUsedBytes)


class TestSpawnCollector(unittest.TestCase):
    """A suite of test cases for the ``spawn_collector`` function"""
    @classmethod
    def setUp(cls):
        """Runs before every test case"""
        cls.vcenter = MagicMock()
        cls.influx = MagicMock()
        cls.unity = MagicMock()
        cls.collectors = {'vms' : {}, 'esxi_hosts' : {}, 'unity' : {}}

    @classmethod
    def tearDown(cls):
        """Runs after every test case"""
        cls.vcenter = None
        cls.influx = None
        cls.unity = None
        cls.collectors = None

    @patch.object(collect_inf_stats, 'VMCollector')
    def test_spawn_vm_collector(self, fake_VMCollector):
        """``spawn_collector`` can create a VMCollector object"""
        fake_vm_collector = MagicMock()
        fake_VMCollector.return_value = fake_vm_collector
        collectors = collect_inf_stats.spawn_collector(self.vcenter,
                                                       self.influx,
                                                       self.unity,
                                                       'someVM',
                                                       self.collectors,
                                                       kind='vms')
        collector = collectors['vms']['someVM']

        self.assertTrue(collector is fake_vm_collector)
        self.assertTrue(fake_vm_collector.start.called)

    @patch.object(collect_inf_stats, 'ESXiCollector')
    def test_spawn_esxi_collector(self, fake_ESXiCollector):
        """``spawn_collector`` can create a VMCollector object"""
        fake_esxi_collector = MagicMock()
        fake_ESXiCollector.return_value = fake_esxi_collector
        collectors = collect_inf_stats.spawn_collector(self.vcenter,
                                                       self.influx,
                                                       self.unity,
                                                       'someESXiHost',
                                                       self.collectors,
                                                       kind='esxi_hosts')
        collector = collectors['esxi_hosts']['someESXiHost']

        self.assertTrue(collector is fake_esxi_collector)
        self.assertTrue(fake_esxi_collector.start.called)

    @patch.object(collect_inf_stats, 'UnityCollector')
    def test_spawn_unity_collector(self, fake_UnityCollector):
        """``spawn_collector`` can create a UnityCollector object"""
        fake_unity_collector = MagicMock()
        fake_UnityCollector.return_value = fake_unity_collector
        collectors = collect_inf_stats.spawn_collector(self.vcenter,
                                                       self.influx,
                                                       self.unity,
                                                       'lun_latency',
                                                       self.collectors,
                                                       kind='unity')
        collector = collectors['unity']['lun_latency']

        self.assertTrue(collector is fake_unity_collector)
        self.assertTrue(fake_unity_collector.start.called)


class TestCreateCollectors(unittest.TestCase):
    """A suite of test cases for the ``create_collectors`` function"""
    @classmethod
    def setUp(cls):
        """Runs before every test case"""
        cls.vcenter = MagicMock()
        cls.influx = MagicMock()
        cls.unity = MagicMock()
        cls.log = MagicMock()
        cls.collectors = {'vms' : {}, 'esxi_hosts' : {}, 'unity' : {}}

    @classmethod
    def tearDown(cls):
        """Runs after every test case"""
        cls.vcenter = None
        cls.influx = None
        cls.unity = None
        cls.log = None
        cls.collectors = None

    @patch.object(collect_inf_stats, 'spawn_collector')
    def test_spawns_all_the_things(self, fake_spawn_collector):
        """``spawn_collectors`` spawns all collects that are not already created"""
        fake_spawn_collector.return_value = 'someCollector'
        self.vcenter.host_systems.keys.return_value = ['hostA', 'hostB']

        collect_inf_stats.create_collectors(self.vcenter,
                                            self.influx,
                                            self.unity,
                                            self.collectors,
                                            self.log)

        collectors_created = fake_spawn_collector.call_count
        expected = 11

        self.assertEqual(collectors_created, expected)


class TestRespawnCollectors(unittest.TestCase):
    """A suite of test cases for the ``respawn_collectors`` function"""
    @classmethod
    def setUp(cls):
        """Runs before every test case"""
        cls.vcenter = MagicMock()
        cls.influx = MagicMock()
        cls.unity = MagicMock()
        cls.log = MagicMock()
        fake_collector = MagicMock()
        fake_collector.is_alive.return_value = False
        cls.collectors = {'vms' : {'someVM' : fake_collector},
                          'esxi_hosts' : {'myEsxiHost' : fake_collector},
                          'unity' : {'UnityStat' : fake_collector}}

    @classmethod
    def tearDown(cls):
        """Runs after every test case"""
        cls.vcenter = None
        cls.influx = None
        cls.unity = None
        cls.log = None
        cls.collectors = None

    @patch.object(collect_inf_stats, 'spawn_collector')
    def test_respawn_collectors(self, fake_spawn_collector):
        """``respawn_collectors`` respawns all dead collectors"""
        collect_inf_stats.respawn_collectors(self.vcenter,
                                             self.influx,
                                             self.unity,
                                             self.collectors,
                                             self.log)

        expected = 3
        self.assertEqual(fake_spawn_collector.call_count, expected)


class TestMain(unittest.TestCase):
    """A suite of unit tests for the ``main`` function"""
    @patch.object(collect_inf_stats, 'respawn_collectors')
    @patch.object(collect_inf_stats, 'create_collectors')
    @patch.object(collect_inf_stats, 'time')
    @patch.object(collect_inf_stats, 'get_logger')
    @patch.object(collect_inf_stats, 'vCenter')
    @patch.object(collect_inf_stats, 'InfluxDB')
    @patch.object(collect_inf_stats, 'Unity')
    def test_main(self, fake_Unity, fake_InfluxDB, fake_vCenter, fake_get_logger,
                  fake_time, fake_create_collectors, fake_respawn_collectors):
        """``main`` TODO"""
        # breaks the 'while True' loop of main
        fake_time.sleep.side_effect = [NotImplementedError('testing')]

        with self.assertRaises(NotImplementedError):
            collect_inf_stats.main('influx_server', 'influx_user', 'influx_password',
                                   'vcenter_server', 'vcenter_user', 'vcenter_password',
                                   'unity_server', 'unity_user', 'unity_password')

        self.assertTrue(fake_Unity.called)
        self.assertTrue(fake_InfluxDB.called)
        self.assertTrue(fake_vCenter.called)
        self.assertTrue(fake_get_logger.called)
        self.assertTrue(fake_create_collectors.called)
        self.assertTrue(fake_respawn_collectors.called)


if __name__ == '__main__':
    unittest.main()
