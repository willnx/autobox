# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``unity_collectors`` module"""
import unittest
from unittest.mock import patch, MagicMock
import copy
import types
import time
import threading

from stat_collector.lib import unity_collectors


class TestUnityStat(unittest.TestCase):
    """A suite of test cases for the ``UnityStat`` object"""
    @classmethod
    def setUp(cls):
        """Runs before every test case"""
        cls.unity = MagicMock()

    @classmethod
    def tearDown(cls):
        """Runs after every test case"""
        cls.unity = None

    def test_init(self):
        """``UnityStat`` the __init__ params have not changed"""
        stat = unity_collectors.UnityStat(self.unity)

        self.assertTrue(isinstance(stat, unity_collectors.UnityStat))

    def test_repr(self):
        """``UnityStat`` the __repr__ method provides context to the stat object"""
        self.unity.ip_addr = '1.2.3.4'
        stat = unity_collectors.UnityStat(self.unity)
        stat._STAT_NAME = 'someStat'
        stat._stat_id = 'theStatId'

        repr = '{}'.format(stat)
        expected = 'UnityStat(Server=1.2.3.4, stat=someStat, id=theStatId)'

        self.assertEqual(repr, expected)

    def test_init_stat(self):
        """``UnityStat`` the '_init_stat' method tells Unity to "start collecting that stat" """
        stat = unity_collectors.UnityStat(self.unity)
        stat._init_stat()

        self.assertTrue(self.unity.post.called)
        self.assertTrue(stat._stat_init_done)

    @patch.object(unity_collectors.UnityStat, '_init_stat')
    def test_query_init(self, fake_init_stat):
        """``UnityStat`` the 'query' method will auto-init the stat"""
        stat = unity_collectors.UnityStat(self.unity)

        before_query = copy.copy(stat._stat_init_done)
        stat.query()

        self.assertFalse(before_query)
        self.assertTrue(fake_init_stat.called)

    def test_query(self):
        """``UnityStat`` the 'query' method returns a dictionary of EPOCH times to stat values"""
        fake_resp = MagicMock()
        fake_resp.json.return_value = {'entries' : [{'content' : {'timestamp' : '2019-04-29T15:26:00.000Z', 'values' : 7}}]}
        self.unity.get.return_value = fake_resp
        stat = unity_collectors.UnityStat(self.unity)

        data = stat.query()
        expected = {1556551560: 7}

        self.assertEqual(data, expected)


class TestUnityLunLatency(unittest.TestCase):
    """A suite of test cases for the ``UnityLunLatency`` object"""
    @classmethod
    def setUp(cls):
        """Runs before ever test case"""
        cls.unity = MagicMock()

    @classmethod
    def tearDown(cls):
        """Runs after every test case"""
        cls.unity = None

    def test_process(self):
        """``UnityLunLatency`` 'process' returns a generator"""
        lun_latency = unity_collectors.UnityLunLatency(self.unity)
        fake_stat = {'spa' : {'someLun' : 1234},
                     'spb' : {'someLun' : 2345}}

        data = lun_latency.process(fake_stat)

        self.assertTrue(data, types.GeneratorType)

    def test_process_data(self):
        """``UnityLunLatency`` returns tuples while processing the stat"""
        lun_latency = unity_collectors.UnityLunLatency(self.unity)
        fake_stat = {'spa' : {'someLun' : 1234},
                     'spb' : {'someLun' : 2345}}

        data = [copy.deepcopy(x) for x in lun_latency.process(fake_stat)]
        expected = [({'latency': 1234}, {'kind': 'unity', 'name': 'spa_someLun'}),
                    ({'latency': 2345}, {'kind': 'unity', 'name': 'spb_someLun'})]

        self.assertEqual(data, expected)


class TestUnityLunIO(unittest.TestCase):
    """A suite of test cases for the ``UnityLunIO`` object"""
    @classmethod
    def setUp(cls):
        """Runs before ever test case"""
        cls.unity = MagicMock()

    @classmethod
    def tearDown(cls):
        """Runs after every test case"""
        cls.unity = None

    def test_process(self):
        """``UnityLunIO`` 'process' returns a generator"""
        lun_io = unity_collectors.UnityLunIO(self.unity)
        fake_stat = {'spa' : {'someLun' : 1234},
                     'spb' : {'someLun' : 2345}}

        data = lun_io.process(fake_stat)

        self.assertTrue(data, types.GeneratorType)

    def test_process_data(self):
        """``UnityLunIO`` returns tuples while processing the stat"""
        lun_io = unity_collectors.UnityLunIO(self.unity)
        fake_stat = {'spa' : {'someLun' : 1234},
                     'spb' : {'someLun' : 2345}}

        data = [copy.deepcopy(x) for x in lun_io.process(fake_stat)]
        expected = [({'iops': 1234}, {'kind': 'unity', 'name': 'spa_someLun'}),
                    ({'iops': 2345}, {'kind': 'unity', 'name': 'spb_someLun'})]

        self.assertEqual(data, expected)


class TestUnityNetBytesIn(unittest.TestCase):
    """A suite of test cases for the ``UnityNetBytesIn`` object"""
    @classmethod
    def setUp(cls):
        """Runs before ever test case"""
        cls.unity = MagicMock()

    @classmethod
    def tearDown(cls):
        """Runs after every test case"""
        cls.unity = None

    def test_process(self):
        """``UnityNetBytesIn`` 'process' returns a generator"""
        net_bytes_in = unity_collectors.UnityNetBytesIn(self.unity)
        fake_stat = {'spa' : {'someLun' : 1234},
                     'spb' : {'someLun' : 2345}}

        data = net_bytes_in.process(fake_stat)

        self.assertTrue(data, types.GeneratorType)

    def test_process_data(self):
        """``UnityNetBytesIn`` returns tuples while processing the stat"""
        net_bytes_in = unity_collectors.UnityNetBytesIn(self.unity)
        fake_stat = {'spa' : {'spa_eth0' : 1234},
                     'spb' : {'spb_eth0' : 2345}}

        data = [copy.deepcopy(x) for x in net_bytes_in.process(fake_stat)]
        expected = [({'bytes_in': 1234}, {'kind': 'unity', 'name': 'spa_eth0'}),
                    ({'bytes_in': 2345}, {'kind': 'unity', 'name': 'spb_eth0'})]

        self.assertEqual(data, expected)

    def test_process_data_with_values(self):
        """``UnityNetBytesIn`` doesn't bother with nics that have no throughput"""
        net_bytes_out = unity_collectors.UnityNetBytesIn(self.unity)
        fake_stat = {'spa' : {'spa_eth0' : 0},
                     'spb' : {'spb_eth0' : 0}}

        data = [copy.deepcopy(x) for x in net_bytes_out.process(fake_stat)]
        expected = []

        self.assertEqual(data, expected)


class TestUnityNetBytesOut(unittest.TestCase):
    """A suite of test cases for the ``UnityNetBytesOut`` object"""
    @classmethod
    def setUp(cls):
        """Runs before ever test case"""
        cls.unity = MagicMock()

    @classmethod
    def tearDown(cls):
        """Runs after every test case"""
        cls.unity = None

    def test_process(self):
        """``UnityNetBytesOut`` 'process' returns a generator"""
        net_bytes_out = unity_collectors.UnityNetBytesOut(self.unity)
        fake_stat = {'spa' : {'someLun' : 1234},
                     'spb' : {'someLun' : 2345}}

        data = net_bytes_out.process(fake_stat)

        self.assertTrue(data, types.GeneratorType)

    def test_process_data(self):
        """``UnityNetBytesOut`` returns tuples while processing the stat"""
        net_bytes_out = unity_collectors.UnityNetBytesOut(self.unity)
        fake_stat = {'spa' : {'spa_eth0' : 1234},
                     'spb' : {'spb_eth0' : 2345}}

        data = [copy.deepcopy(x) for x in net_bytes_out.process(fake_stat)]
        expected = [({'bytes_out': 1234}, {'kind': 'unity', 'name': 'spa_eth0'}),
                    ({'bytes_out': 2345}, {'kind': 'unity', 'name': 'spb_eth0'})]

        self.assertEqual(data, expected)

    def test_process_data_with_values(self):
        """``UnityNetBytesOut`` doesn't bother with nics that have no throughput"""
        net_bytes_out = unity_collectors.UnityNetBytesOut(self.unity)
        fake_stat = {'spa' : {'spa_eth0' : 0},
                     'spb' : {'spb_eth0' : 0}}

        data = [copy.deepcopy(x) for x in net_bytes_out.process(fake_stat)]
        expected = []

        self.assertEqual(data, expected)


class TestUnityMemoryUsedBytes(unittest.TestCase):
    """A suite of test cases for the ``UnityNetBytesOut`` object"""
    @classmethod
    def setUp(cls):
        """Runs before ever test case"""
        cls.unity = MagicMock()

    @classmethod
    def tearDown(cls):
        """Runs after every test case"""
        cls.unity = None

    def test_process(self):
        """``UnityMemoryUsedBytes`` 'process' returns a generator"""
        ram = unity_collectors.UnityMemoryUsedBytes(self.unity)
        fake_stat = {'spa' : {'someLun' : 1234},
                     'spb' : {'someLun' : 2345}}

        data = ram.process(fake_stat)

        self.assertTrue(data, types.GeneratorType)

    def test_process_data(self):
        """``UnityMemoryUsedBytes`` returns tuples while processing the stat"""
        ram = unity_collectors.UnityMemoryUsedBytes(self.unity)
        fake_stat = {'spa' : 1234, 'spb' : 2345}

        data = [copy.deepcopy(x) for x in ram.process(fake_stat)]
        expected = [({'ram_active': 1234}, {'kind': 'unity', 'name': 'spa'}),
                    ({'ram_active': 2345}, {'kind': 'unity', 'name': 'spb'})]

        self.assertEqual(data, expected)


class TestUnityCollector(unittest.TestCase):
    """A suite of test cases for the ``UnityCollector`` object"""
    @classmethod
    def setUp(cls):
        """Runs before every test case"""
        cls.influx = MagicMock()
        cls.unity = MagicMock()
        cls.stat = MagicMock()
        cls.stat.query.return_value = {'1234' : 'some Value'}
        cls.stat.process.return_value = [({'field' : 'foo'}, {'tags' : "fooAgain"})]

    @classmethod
    def tearDown(cls):
        """Runs after every tets case"""
        cls.influx = None
        cls.unity = None
        cls.stat = None

    def test_init(self):
        """``UnityCollector`` is a thread"""
        collector = unity_collectors.UnityCollector(self.influx, self.unity, self.stat)

        self.assertTrue(isinstance(collector, threading.Thread))

    @patch.object(unity_collectors, 'time')
    def test_run(self, fake_time):
        """``UnityCollector`` runs on a loop to collect and upload data"""
        fake_time.time.return_value = 42
        collector = unity_collectors.UnityCollector(self.influx, self.unity, self.stat)

        collector.start()
        time.sleep(1) # avoid race between test, and thread
        collector.keep_running = False
        collector.join()

        self.assertTrue(self.stat.query.called)
        self.assertTrue(self.influx.write.called)



if __name__ == '__main__':
    unittest.main()
