# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``scheduler.py`` module"""
import unittest

from cronner import elasticsearch
from cronner.scheduler import schedule


class TestSchedule(unittest.TestCase):
    """A suite of test cases for the ``schedule`` object"""
    @classmethod
    def setUpClass(cls):
        """Runs once for the entire suite"""
        # Avoid magic numbers when inspecting the schedule object
        # Order matter, so as you add functions to schedule remember
        # to increment the magic number value
        cls.PRUNE_INDICES = 0
        cls.ADD_FIELD_DATA =1

    def _verify_func(self, func, func_index):
        job = schedule.jobs[func_index]
        if not job.job_func.func is func:
            error = 'Detected change in job ordering of ``schedule`` object'
            error += '\nFailing test'
            raise RuntimeError(error)
        else:
            return job

    def test_prune_indices(self):
        """``schedule`` the 'prune_indices' schedule has not changed"""
        job = self._verify_func(elasticsearch.prune_indices, self.PRUNE_INDICES)

        run_schedule = '{}'.format(job)[:26]
        expected = 'Every 1 day at 01:00:00 do'

        self.assertEqual(run_schedule, expected)

    def test_add_field_data(self):
        """``schedule`` the 'prune_indices' schedule has not changed"""
        job = self._verify_func(elasticsearch.add_field_data, self.ADD_FIELD_DATA)

        run_schedule = '{}'.format(job)[:19]
        expected = 'Every 10 minutes do'

        self.assertEqual(run_schedule, expected)



if __name__ == '__main__':
    unittest.main()
