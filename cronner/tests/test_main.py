# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``main.py`` module"""
import unittest
from unittest.mock import patch

from cronner import main


class TestMain(unittest.TestCase):
    """A suite of test cases for the ``main`` function"""
    @patch.object(main, 'schedule')
    @patch.object(main.time, 'sleep')
    def test_main(self, fake_sleep, fake_schedule):
        """``main`` sleeps for 1 second in between running pending jobs"""
        fake_sleep.side_effect = [NotImplementedError('testing')]

        try:
            main.main()
        except NotImplementedError:
            pass

        the_args, _ = fake_sleep.call_args
        slept_for = the_args[0]
        expected = 1

        self.assertEqual(slept_for, expected)
        self.assertTrue(fake_schedule.run_pending.called)


if __name__ == '__main__':
    unittest.main()
