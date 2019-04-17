# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``vsphere_collectors`` module"""
import unittest
from unittest.mock import patch, MagicMock
import threading

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


if __name__ == '__main__':
    unittest.main()
