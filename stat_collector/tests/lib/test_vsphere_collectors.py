# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``vsphere_collectors`` module"""
import unittest
from unittest.mock import patch, MagicMock
import time
import threading
import datetime

from stat_collector.lib import vsphere_collectors

class FakeVirtualMachine(MagicMock):
    def __sublcasscheck__(self, subclass):
        """So we can deal with the requirement to use ``isinstance`` with pyVmomi..."""
        return True


class TestUserCollector(unittest.TestCase):
    """A suite of test cases for the ``UserCollector`` object"""
    @classmethod
    def setUp(cls):
        cls.fake_vcenter = MagicMock()
        cls.fake_influx = MagicMock()

    def test_init(self):
        """``UserCollector`` is a subclass of threading.Thread"""
        uc = vsphere_collectors.UserCollector(self.fake_vcenter, 'someuser', 'users_dir', self.fake_influx)

        self.assertTrue(isinstance(uc, threading.Thread))

    def test_folder(self):
        """``UserCollector`` the 'folder' property refreshes the view of the user's folder"""
        fake_folder = MagicMock()
        fake_folder.name = 'someuser'
        fake_parent_folder = MagicMock()
        fake_parent_folder.childEntity = [fake_folder]
        self.fake_vcenter.get_by_name.return_value = fake_parent_folder

        uc = vsphere_collectors.UserCollector(self.fake_vcenter, 'someuser', 'users_dir', self.fake_influx)
        folder = uc.folder

        self.assertTrue(folder is fake_folder)

    def test_no_folder(self):
        """``UserCollector`` the 'folder' property raises RuntimeError if unable to find the specific user folder"""
        uc = vsphere_collectors.UserCollector(self.fake_vcenter, 'someuser', 'users_dir', self.fake_influx)

        with self.assertRaises(RuntimeError):
            uc.folder

    @patch.object(vsphere_collectors, 'vim')
    @patch.object(vsphere_collectors.UserCollector, 'folder')
    def test_get_usage(self, fake_folder, fake_vim):
        """``UserCollector`` returns a dictionary of fields and tags for writing to InfluxDB"""
        fake_vim.VirtualMachine = FakeVirtualMachine
        fake_entity = FakeVirtualMachine()
        fake_entity.runtime.powerState = 'poweredOn'
        fake_entity.config.annotation = '{"component":"OneFS"}'
        fake_folder.childEntity = [fake_entity]

        uc = vsphere_collectors.UserCollector(self.fake_vcenter, 'someuser', 'users_dir', self.fake_influx)
        data = uc.get_usage()
        expected = {'fields': {'total_vms': 1, 'powered_on': 1, 'username': '"someuser"', 'OneFS': 1}, 'tags': {'user': 'someuser'}}

        self.assertEqual(data, expected)

    @patch.object(vsphere_collectors, 'vim')
    @patch.object(vsphere_collectors.UserCollector, 'folder')
    def test_get_usage_bad_json(self, fake_folder, fake_vim):
        """``UserCollector`` 'get_usage' handles invalid meta-data on the VM (i.e. while it's being deployed)"""
        fake_vim.VirtualMachine = FakeVirtualMachine
        fake_entity = FakeVirtualMachine()
        fake_entity.runtime.powerState = 'poweredOff'
        fake_entity.config.annotation = '{Not JSON'
        fake_folder.childEntity = [fake_entity]

        uc = vsphere_collectors.UserCollector(self.fake_vcenter, 'someuser', 'users_dir', self.fake_influx)
        data = uc.get_usage()
        expected = {'fields': {'total_vms': 1, 'powered_on': 0, 'username': '"someuser"', 'deploying': 1}, 'tags': {'user': 'someuser'}}

        self.assertEqual(data, expected)

    @patch.object(vsphere_collectors.time, 'sleep')
    @patch.object(vsphere_collectors, 'randint')
    def test_run(self, fake_randint, fake_sleep):
        """``UserCollector`` 'run' terminates upon catching an exception"""
        fake_folder = MagicMock()
        fake_folder.name = 'someuser'
        fake_parent_folder = MagicMock()
        fake_parent_folder.childEntity = [fake_folder]
        self.fake_vcenter.get_by_name.return_value = fake_parent_folder
        fake_log = MagicMock()
        fake_randint.return_value = 0
        self.fake_influx.write.side_effect = [None, RuntimeError('testing')]
        uc = vsphere_collectors.UserCollector(self.fake_vcenter, 'someuser', 'users_dir', self.fake_influx)
        uc.log = fake_log

        uc.run()

        self.assertTrue(fake_log.exception.called)


    @patch.object(vsphere_collectors.time, 'sleep')
    @patch.object(vsphere_collectors, 'randint')
    def test_run_random(self, fake_randint, fake_sleep):
        """``UserCollector`` 'run' sleeps for a random amount of time before running"""
        fake_folder = MagicMock()
        fake_folder.name = 'someuser'
        fake_parent_folder = MagicMock()
        fake_parent_folder.childEntity = [fake_folder]
        self.fake_vcenter.get_by_name.return_value = fake_parent_folder
        fake_log = MagicMock()
        fake_randint.return_value = 0
        self.fake_influx.write.side_effect = [RuntimeError('testing')]
        uc = vsphere_collectors.UserCollector(self.fake_vcenter, 'someuser', 'users_dir', self.fake_influx)
        uc.log = fake_log

        uc.run()

        self.assertTrue(fake_randint.called)
        self.assertEqual(2, fake_sleep.call_count) # upon calling 'run', then at the end of the 1st loop


class TestPerfCollector(unittest.TestCase):
    """A suite of test cases for the ``PerfCollector`` object"""
    @classmethod
    def setUp(cls):
        """Runs before every test case"""
        cls.vcenter = MagicMock()
        cls.entity = MagicMock()

    @classmethod
    def tearDown(cls):
        """Runs after every test case"""
        cls.vcenter = None
        cls.entity = None

    def test_init(self):
        """``PerfCollector`` the init parameters have not changed"""
        counter = 'some_vmware_stat'
        perfc = vsphere_collectors.PerfCollector(self.vcenter, self.entity, counter)

        self.assertTrue(perfc.vcenter is self.vcenter)
        self.assertTrue(perfc.entity is self.entity)
        self.assertTrue(perfc.counter_name is counter)

    def test_repr(self):
        """``PerfCollector`` the __repr__ contains metric context"""
        self.entity.name = 'someStat'
        perfc = vsphere_collectors.PerfCollector(self.vcenter, self.entity, 'some_vmware_stat')
        time_val = perfc.last_collected.strftime('%Y/%m/%d %H:%M:%S')

        repr = '{}'.format(perfc)
        expected = 'PerfCollector(name=someStat, stat=some_vmware_stat, last={})'.format(time_val)

        self.assertEqual(repr, expected)

    def test_perf_manager(self):
        """``PerfCollector`` the 'perf_manager' object is obtained form the vcenter content"""
        fake_perf_mgr = MagicMock()
        self.vcenter.content.perfManager = fake_perf_mgr
        perfc = vsphere_collectors.PerfCollector(self.vcenter, self.entity, 'some_vmware_stat')

        perf_mgr = perfc.perf_manager

        self.assertTrue(perf_mgr is fake_perf_mgr)

    def test_counters(self):
        """``PerfCollector`` the 'counters' property returns a dictionary"""
        fake_counter = MagicMock()
        fake_counter.key = 42
        fake_counter.groupInfo.key = 'someGrp'
        fake_counter.nameInfo.key = 'someStat'
        fake_counter.rollupType = 'someCategory'
        self.vcenter.content.perfManager.perfCounter = [fake_counter]
        perfc = vsphere_collectors.PerfCollector(self.vcenter, self.entity, 'some_vmware_stat')

        counters = perfc.counters
        expected = {'someGrp.someStat.someCategory' : 42}

        self.assertEqual(counters, expected)

    @patch.object(vsphere_collectors.vim.PerformanceManager, 'MetricId')
    def test_metric_id(self, fake_MetricId):
        """``PerfCollector`` the 'metric_id' property is only generated once"""
        fake_counter = MagicMock()
        fake_counter.key = 42
        fake_counter.groupInfo.key = 'someGrp'
        fake_counter.nameInfo.key = 'someStat'
        fake_counter.rollupType = 'someCategory'
        self.vcenter.content.perfManager.perfCounter = [fake_counter]
        perfc = vsphere_collectors.PerfCollector(self.vcenter, self.entity, 'someGrp.someStat.someCategory')

        perfc.metric_id
        perfc.metric_id

        self.assertEqual(fake_MetricId.call_count, 1)

    @patch.object(vsphere_collectors.vim.PerformanceManager, 'QuerySpec')
    def test_query_no_data(self, fake_QuerySpec):
        """``PerfCollector`` the 'query' method returns an empty dictionary if no data is available"""
        fake_perf_mgr = MagicMock()
        fake_perf_mgr.QueryPerf.return_value = []
        self.vcenter.content.perfManager = fake_perf_mgr
        perfc = vsphere_collectors.PerfCollector(self.vcenter, self.entity, 'someGrp.someStat.someCategory')
        perfc._metric_id = ['someGrp.someStat.someCategory']

        stats = perfc.query()
        expected = {}

        self.assertEqual(stats, expected)

    @patch.object(vsphere_collectors.vim.PerformanceManager, 'QuerySpec')
    def test_query(self, fake_QuerySpec):
        """``PerfCollector`` the 'query' method returns a sane datastructure"""
        fake_data_value = MagicMock()
        fake_data_value.value = [42]
        fake_data_time = MagicMock()
        fake_data_time.timestamp = datetime.datetime.now()
        fake_data = MagicMock()
        fake_data.sampleInfo = [fake_data_time]
        fake_data.value = [fake_data_value]
        fake_perf_mgr = MagicMock()
        fake_perf_mgr.QueryPerf.return_value = [fake_data]
        self.vcenter.content.perfManager = fake_perf_mgr
        perfc = vsphere_collectors.PerfCollector(self.vcenter, self.entity, 'someGrp.someStat.someCategory')
        perfc._metric_id = ['someGrp.someStat.someCategory']

        stats = perfc.query()
        time_in_seconds = int(fake_data_time.timestamp.strftime('%s'))
        epoch = time_in_seconds - time.timezone
        expected = {epoch : 42}

        self.assertEqual(stats, expected)

    @patch.object(vsphere_collectors.vim.PerformanceManager, 'QuerySpec')
    def test_query_index_error(self, fake_QuerySpec):
        """``PerfCollector`` the 'query' handles the shit partial response from pyVmomi"""
        fake_data_value = MagicMock()
        fake_data_value.value = [42] # one value, but two time stamps...
        fake_data_time = MagicMock()
        fake_data_time.timestamp = datetime.datetime.now()
        fake_data = MagicMock()
        fake_data.sampleInfo = [fake_data_time, fake_data_time] # two time stamps, but only 1 value...
        fake_data.value = [fake_data_value]
        fake_perf_mgr = MagicMock()
        fake_perf_mgr.QueryPerf.return_value = [fake_data]
        self.vcenter.content.perfManager = fake_perf_mgr
        perfc = vsphere_collectors.PerfCollector(self.vcenter, self.entity, 'someGrp.someStat.someCategory')
        perfc._metric_id = ['someGrp.someStat.someCategory']

        stats = perfc.query()
        time_in_seconds = int(fake_data_time.timestamp.strftime('%s'))
        epoch = time_in_seconds - time.timezone
        expected = {epoch : 42}

        self.assertEqual(stats, expected)


class TestCollectorThread(unittest.TestCase):
    """A suite of test cases for the ``CollectorThread`` object"""
    @classmethod
    def setUp(cls):
        """Runs before every test case"""
        cls.vcenter = MagicMock()
        cls.influx = MagicMock()

    @classmethod
    def tearDown(cls):
        """Runs after every test case"""
        cls.vcenter = None
        cls.influx = None

    def test_init(self):
        """``CollectorThread`` '__init__' params have not changed"""
        collector = vsphere_collectors.CollectorThread(self.vcenter, self.influx, 'someThingInVMware')

        self.assertTrue(isinstance(collector, vsphere_collectors.CollectorThread))

    def test_setup_collectors(self):
        """``CollectorThread`` 'setup_collectors' sets the 'collectors' attr on the object"""
        collector = vsphere_collectors.CollectorThread(self.vcenter, self.influx, 'someThingInVMware')

        collector.collectors = MagicMock() # explictly setting it here, so we can test if it gets changed
        collector.setup_collectors()
        expected = []

        self.assertEqual(collector.collectors, expected)

    def test_entity(self):
        """``CollectorThread`` the entity property returns the VMware object"""
        collector = vsphere_collectors.CollectorThread(self.vcenter, self.influx, 'someThingInVMware')
        collector.find_entity = lambda: 'someEntity'

        entity = collector.entity()
        expected = 'someEntity'

        self.assertEqual(entity, expected)

    def test_entity_cached(self):
        """``CollectorThread`` the entity property caches the object, instead of looking it up constantly"""
        fake_find_entity = MagicMock()
        collector = vsphere_collectors.CollectorThread(self.vcenter, self.influx, 'someThingInVMware')
        collector.find_entity = fake_find_entity

        collector.entity()
        collector.entity()
        collector.entity()
        collector.entity()

        actual_calls = fake_find_entity.call_count
        expected_calls = 1

        self.assertEqual(actual_calls, expected_calls)

    def test_collect_stats(self):
        """``CollectorThread`` 'collect_stats' queries for data, then uploads to InfluxDB"""
        fake_stat_collector = MagicMock()
        fake_stat_collector.counter_name = 'someStat'
        fake_stat_collector.query.return_value = { 123456789 : 42 }
        collector = vsphere_collectors.CollectorThread(self.vcenter, self.influx, 'someThingInVMware')
        collector._stats = ['someStat']
        collector.collectors = [fake_stat_collector]

        collector.collect_stats()
        _, the_kwargs = self.influx.write.call_args
        expected = {'fields': {'someStat': 42}, 'tags': {'name': 'someThingInVMware', 'kind': 'None'}, 'timestamp': 123456789}

        self.assertEqual(the_kwargs, expected)

    def test_collect_stats_setup(self):
        """``CollectorThread`` 'collect_stats' will setup the collectors if needed"""
        fake_stat_collector = MagicMock()
        fake_stat_collector.counter_name = 'someStat'
        fake_stat_collector.query.return_value = { 123456789 : 42 }
        fake_setup_collectors = MagicMock()
        fake_setup_collectors.return_value = [fake_stat_collector]
        collector = vsphere_collectors.CollectorThread(self.vcenter, self.influx, 'someThingInVMware')
        collector._stats = ['someStat']
        collector.setup_collectors = fake_setup_collectors

        collector.collect_stats()

        self.assertTrue(fake_setup_collectors.called)

    @patch.object(vsphere_collectors, 'time')
    def test_run(self, fake_time):
        """``CollectorThread`` 'run' collects stats, then sleeps"""
        fake_time.time.return_value = 1
        fake_collect_stats = MagicMock()
        collector = vsphere_collectors.CollectorThread(self.vcenter, self.influx, 'someThingInVMware')
        collector.collect_stats = fake_collect_stats

        collector.start()
        collector.keep_running = False
        collector.join()

        self.assertTrue(fake_time.sleep.called)

    @patch.object(vsphere_collectors, 'time')
    def test_run_no_sleep(self, fake_time):
        """``CollectorThread`` 'run' sleeps for zero seconds if the collection took longer than the loop_interval"""
        fake_time.time.side_effect = [x * 500 for x in range(500)]
        fake_collect_stats = MagicMock()
        collector = vsphere_collectors.CollectorThread(self.vcenter, self.influx, 'someThingInVMware')
        collector.collect_stats = fake_collect_stats

        collector.start()
        collector.keep_running = False
        collector.join()

        the_args, _ = fake_time.sleep.call_args
        slept_for = the_args[0]
        expected = 0

        self.assertEqual(slept_for, expected)


class TestVMCollector(unittest.TestCase):
    """A suite of test cases for the ``VMCollector`` object"""
    @classmethod
    def setUp(cls):
        """Runs before every test case"""
        cls.vcenter = MagicMock()
        cls.influx = MagicMock()

    @classmethod
    def tearDown(cls):
        """Runs after every test case"""
        cls.vcenter = None
        cls.influx = None

    @patch.object(vsphere_collectors.VMCollector, 'setup_collectors')
    def test_init(self, fake_setup_collectors):
        """``VMCollector`` the __init__ params have not changed"""
        collector = vsphere_collectors.VMCollector(self.vcenter, self.influx, 'someVM', 'vmParentDir')

        self.assertTrue(isinstance(collector, vsphere_collectors.CollectorThread))

    @patch.object(vsphere_collectors.VMCollector, 'setup_collectors')
    def test_find_entity(self, fake_setup_collectors):
        """``VMCollector`` 'find_entity' searches vCenter for the specific VM """
        fake_entity = MagicMock()
        fake_entity.name = 'someVM'
        fake_folder = MagicMock()
        fake_folder.childEntity = [fake_entity]
        self.vcenter.get_by_name.return_value = fake_folder
        collector = vsphere_collectors.VMCollector(self.vcenter, self.influx, 'someVM', 'vmParentDir')

        entity = collector.find_entity()
        expected = fake_entity

        self.assertTrue(entity is fake_entity)

    @patch.object(vsphere_collectors.VMCollector, 'setup_collectors')
    def test_find_entity_fail(self, fake_setup_collectors):
        """``VMCollector`` 'find_entity' raises a RuntimeError if unable to find the VM"""
        collector = vsphere_collectors.VMCollector(self.vcenter, self.influx, 'someVM', 'vmParentDir')

        with self.assertRaises(RuntimeError):
            collector.find_entity()


class TestESXiCollector(unittest.TestCase):
    """A suite of test cases for the ``ESXiCollector`` object"""
    @classmethod
    def setUp(cls):
        """Runs before every test case"""
        cls.vcenter = MagicMock()
        cls.influx = MagicMock()

    @classmethod
    def tearDown(cls):
        """Runs after every test case"""
        cls.vcenter = None
        cls.influx = None

    @patch.object(vsphere_collectors.ESXiCollector, 'setup_collectors')
    def test_init(self, fake_setup_collectors):
        """``ESXiCollector`` the __init__ params have not changed"""
        collector = vsphere_collectors.ESXiCollector(self.vcenter, self.influx, 'ESXiHost')

        self.assertTrue(isinstance(collector, vsphere_collectors.CollectorThread))

    @patch.object(vsphere_collectors.ESXiCollector, 'setup_collectors')
    def test_find_entity(self, fake_setup_collectors):
        """``ESXiCollector`` 'find_entity' searches vCenter for the specific ESXi host """
        fake_entity = MagicMock()
        self.vcenter.host_systems = {'ESXiHost' : fake_entity}
        collector = vsphere_collectors.ESXiCollector(self.vcenter, self.influx, 'ESXiHost')

        entity = collector.find_entity()
        expected = fake_entity

        self.assertTrue(entity is fake_entity)

    @patch.object(vsphere_collectors.ESXiCollector, 'setup_collectors')
    def test_find_entity_fail(self, fake_setup_collectors):
        """``ESXiCollector`` 'find_entity' raises a RuntimeError if unable to find the ESXi host"""
        self.vcenter.host_systems = {}
        collector = vsphere_collectors.ESXiCollector(self.vcenter, self.influx, 'ESXiHost')

        with self.assertRaises(RuntimeError):
            collector.find_entity()


if __name__ == '__main__':
    unittest.main()
