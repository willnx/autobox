# -*- coding: UTF-8 -*-
"""A suite of unit tests for the ``manager.py`` module"""
import unittest
from unittest.mock import patch, MagicMock
import time
import queue
import multiprocessing

from log_processor import manager


class TestGeneral(unittest.TestCase):
    """A suite of test cases for the ``make_queues`` function and static values"""
    def test_make_queues_count(self):
        """``make_queues`` returns two queues"""
        number_of_queues = len(manager.make_queues())
        expected = 2

        self.assertEqual(number_of_queues, expected)

    def test_make_queues(self):
        """``make_queues`` returns multiprocessing.Queue objects"""
        work_queue, idle_queue = manager.make_queues()

        self.assertTrue(isinstance(work_queue, multiprocessing.queues.Queue))
        self.assertTrue(isinstance(idle_queue, multiprocessing.queues.Queue))

    def test_produce_timeout(self):
        """``PRODUCE_TIMEOUT`` has not changed"""
        expected = 30000

        self.assertEqual(expected, manager.PRODUCE_TIMEOUT)

    def test_produce_interval(self):
        """``PRODUCE_INTERVAL`` has not changed"""
        expected = 30

        self.assertEqual(expected, manager.PRODUCE_INTERVAL)

    def test_produce_before_checking(self):
        """``PRODUCE_BEFORE_CHECKING`` has not changed"""
        expected = 5000

        self.assertEqual(expected, manager.PRODUCE_BEFORE_CHECKING)

    def test_sentinel(self):
        """``SENTINEL`` value used to scale down workers has not changed"""
        expected = 'TERMINATE YOU USELESS PROCESS'

        self.assertEqual(expected, manager.SENTINEL)

    def test_scale_up_by(self):
        """``SCALE_UP_BY`` has not changed"""
        expected = 2

        self.assertEqual(expected, manager.SCALE_UP_BY)

    def test_scale_down_by(self):
        """``SCALE_DOWN_BY`` has not changed"""
        expected = -1

        self.assertEqual(expected, manager.SCALE_DOWN_BY)

    def test_max_workers(self):
        """``MAX_WORKERS`` is x2 the number of CPUs"""
        expected = multiprocessing.cpu_count() * 2

        self.assertEqual(expected, manager.MAX_WORKERS)


@patch.object(manager, 'check_worker_health')
class TestCheckWorkload(unittest.TestCase):
    """A suite of test cases for the ``check_workload`` function"""
    @classmethod
    def setUp(cls):
        """Runs before every test case"""
        cls.log = MagicMock()
        cls.work_queue = multiprocessing.Queue()
        cls.idle_queue = multiprocessing.Queue()

    @classmethod
    def tearDown(cls):
        """Runs after every test case"""
        cls.log = None
        cls._drain_queue(cls.work_queue)
        cls._drain_queue(cls.idle_queue)
        cls.work_queue = None
        cls.idle_queue = None

    @staticmethod
    def _drain_queue(queue):
        """If you don't empty the queues before termination, then you get a BrokePipe traceback"""
        while not queue.empty():
            try:
                queue.get()
            except Exeption:
                pass
        queue.close()
        queue.join_thread()

    def test_check_workload_scale_up(self, fake_check_worker_health):
        """``check_workload`` says to scale up workers by ``SCALE_UP_BY``"""
        for _ in range(200):
            self.work_queue.put('test')
        fake_check_worker_health.return_value = [], 0

        _, needed_workers = manager.check_workload([], self.work_queue, self.idle_queue, self.log)
        expected = manager.SCALE_UP_BY

        self.assertEqual(needed_workers, expected)

    def test_check_workload_scale_down(self, fake_check_worker_health):
        """``check_workload`` says to scale down workers by ``SCALE_DOWN_BY``"""
        fake_check_worker_health.return_value = [], 0

        _, needed_workers = manager.check_workload([], self.work_queue, self.idle_queue, self.log)
        expected = manager.SCALE_DOWN_BY

        self.assertEqual(needed_workers, expected)

    def test_check_workload_scalier(self, fake_check_worker_health):
        """``check_workload`` applies a scalier to replace dead workers"""
        for _ in range(200):
            self.work_queue.put('test')
        fake_check_worker_health.return_value = [], 1

        _, needed_workers = manager.check_workload([], self.work_queue, self.idle_queue, self.log)
        expected = manager.SCALE_UP_BY + 1

        self.assertEqual(needed_workers, expected)


class TestCheckWorkerHealth(unittest.TestCase):
    """A suite of test cases for the ``check_worker_health`` function"""
    @classmethod
    def setUp(cls):
        cls.idle_queue = multiprocessing.Queue()
        cls.log = MagicMock()
        cls.workers = cls._make_workers()

    @classmethod
    def tearDown(cls):
        while not cls.idle_queue.empty():
            try:
                cls.idle_queue.get()
            except Exception:
                pass
        cls.idle_queue.close()
        cls.idle_queue.join_thread()
        cls.idle_queue = None
        cls.log = None
        cls.workers = None

    @staticmethod
    def _make_workers():
        workers = []
        for name in ('worker-1', 'worker-2'):
            worker = MagicMock()
            worker.name = name
            workers.append(worker)
        return workers

    def test_no_errors(self):
        """``check_worker_health`` returns a scalier of zero when no error workers found"""
        _, scalier = manager.check_worker_health(self.workers, self.idle_queue, self.log)
        expected = 0

        self.assertEqual(scalier, expected)

    def test_one_error(self):
        """``check_worker_health`` returns a scalier of 1 when a dead worker is found"""
        message = ('worker-1', 'test error')
        self.idle_queue.put(message)
        time.sleep(0.1) # avoid race between putting into queue, and checking

        _, scalier = manager.check_worker_health(self.workers, self.idle_queue, self.log)
        expected = 1

        self.assertEqual(scalier, expected)

    def test_all_error(self):
        """``check_worker_health`` raises RuntimeError if all workers are dead"""
        message1 = ('worker-1', 'test error')
        message2 = ('worker-2', 'test error')
        self.idle_queue.put(message1)
        self.idle_queue.put(message2)
        time.sleep(0.1) # avoid race between putting into queue, and checking

        with self.assertRaises(RuntimeError):
            _, scalier = manager.check_worker_health(self.workers, self.idle_queue, self.log)

    def test_retired(self):
        """``check_worker_health`` doesn't increase the scalier when it finds a retired worker"""
        message = ('worker-1', '')
        self.idle_queue.put(message)
        time.sleep(0.1) # avoid race between putting into queue, and checking

        _, scalier = manager.check_worker_health(self.workers, self.idle_queue, self.log)
        expected = 0

        self.assertEqual(scalier, expected)

    def test_returned_workers(self):
        """``check_worker_health`` returns a list of only alive workers"""
        message = ('worker-1', '')
        self.idle_queue.put(message)
        time.sleep(0.2) # avoid race between putting into queue, and checking

        workers, _ = manager.check_worker_health(self.workers, self.idle_queue, self.log)
        alive_worker = self.workers.pop()
        expected_workers = [alive_worker]

        self.assertEqual(workers, expected_workers)

    def test_non_blocking_queue(self):
        """``check_worker_health`` does not indefinitely block on the idle_queue"""
        fake_queue = MagicMock()
        fake_queue.empty.return_value = False
        fake_queue.get.side_effect = queue.Empty('testing')
        self.idle_queue = fake_queue

        manager.check_worker_health(self.workers, self.idle_queue, self.log)

        get_called = fake_queue.get.called
        _, kwargs = fake_queue.get.call_args
        blocking = kwargs['block']

        self.assertTrue(get_called)
        self.assertFalse(blocking)


class TestAdjustWOrkerCount(unittest.TestCase):
    """A suite of test cases for the ``adjust_worker_count`` function"""
    def test_keeps_at_least_one(self):
        """``adjust_worker_count`` scales down unless there's only 1 worker"""
        fake_work_queue = MagicMock()
        fake_idle_queue = MagicMock()
        worker_cls = MagicMock()
        work_group = 'testGroup'
        topic = 'testTopic'
        workers = ['someWorker']
        need = -1

        manager.adjust_worker_count(workers, worker_cls, work_group, fake_work_queue, fake_idle_queue, need)

        sent_sentinel = fake_work_queue.put.called

        self.assertFalse(sent_sentinel)

    def test_keeps_signals_term(self):
        """``adjust_worker_count`` notifies a worker to retire when there's more than 1 worker"""
        fake_work_queue = MagicMock()
        fake_idle_queue = MagicMock()
        worker_cls = MagicMock()
        work_group = 'testGroup'
        topic = 'testTopic'
        workers = ['someWorker-1', 'someWorker-2']
        need = -1

        manager.adjust_worker_count(workers, worker_cls, work_group, fake_work_queue, fake_idle_queue, need)

        sent_sentinel = fake_work_queue.put.called

        self.assertTrue(sent_sentinel)

    def test_scales_up(self):
        """``adjust_worker_count`` scales workers up by the value of the 'need' param"""
        fake_work_queue = MagicMock()
        fake_idle_queue = MagicMock()
        worker_cls = MagicMock()
        work_group = 'testGroup'
        topic = 'testTopic'
        workers = ['someWorker-1']
        need = 2

        workers = manager.adjust_worker_count(workers, worker_cls, work_group, fake_work_queue, fake_idle_queue, need)
        active_workers = len(workers)
        expected = 3

        self.assertEqual(active_workers, expected)


class TestProduceWork(unittest.TestCase):
    """A suite of test cases for the ``produce_work`` function"""
    @patch.object(manager, 'check_workload')
    @patch.object(manager, 'adjust_worker_count')
    def test_produce_before_check(self, fake_adjust_worker_count, fake_check_workload):
        """``produce_work`` puts items into the work_queue until PRODUCE_BEFORE_CHECKING count is met"""
        fake_event = MagicMock()
        fake_event.value = 'eventA'
        workers = []
        worker_cls = MagicMock()
        work_group = 'someGroup'
        topic = 'someTopic'
        kafka = [fake_event, fake_event]
        work_queue = MagicMock()
        idle_queue = MagicMock()
        log = MagicMock()

        manager.produce_work(workers, worker_cls, work_group, topic, work_queue, idle_queue, kafka, log)

        self.assertFalse(fake_adjust_worker_count.called)
        self.assertFalse(fake_check_workload.called)

    @patch.object(manager.time, 'time')
    @patch.object(manager, 'check_workload')
    @patch.object(manager, 'adjust_worker_count')
    def test_produce_interval(self, fake_adjust_worker_count, fake_check_workload, fake_time):
        """``produce_work`` keeps producing after PRODUCE_BEFORE_CHECKING until PRODUCE_INTERVAL amount of time has passed"""
        fake_time.side_effect = [1234, 1235]
        fake_event = MagicMock()
        fake_event.value = 'eventA'
        workers = []
        worker_cls = MagicMock()
        work_group = 'someGroup'
        topic = 'someTopic'
        kafka = [fake_event] * manager.PRODUCE_BEFORE_CHECKING
        work_queue = MagicMock()
        idle_queue = MagicMock()
        log = MagicMock()

        manager.produce_work(workers, worker_cls, work_group, topic, work_queue, idle_queue, kafka, log)
        time_checked = fake_time.call_count
        expected = 2 # once at the start of the func, then once after PRODUCE_BEFORE_CHECKING is met

        self.assertFalse(fake_adjust_worker_count.called)
        self.assertFalse(fake_check_workload.called)
        self.assertEqual(time_checked, expected)

    @patch.object(manager.time, 'time')
    @patch.object(manager, 'check_workload')
    @patch.object(manager, 'adjust_worker_count')
    def test_scales_workers(self, fake_adjust_worker_count, fake_check_workload, fake_time):
        """``produce_work`` scales workers after PRODUCE_BEFORE_CHECKING and PRODUCE_INTERVAL amount of time has passed"""
        fake_time.side_effect = [0, 90000000, 900000000]
        fake_check_workload.return_value = ([], 0)
        fake_adjust_worker_count.return_value = []
        fake_event = MagicMock()
        fake_event.value = 'eventA'
        workers = []
        worker_cls = MagicMock()
        work_group = 'someGroup'
        topic = 'someTopic'
        kafka = [fake_event] * manager.PRODUCE_BEFORE_CHECKING
        work_queue = MagicMock()
        idle_queue = MagicMock()
        log = MagicMock()

        manager.produce_work(workers, worker_cls, work_group, topic, work_queue, idle_queue, kafka, log)

        self.assertTrue(fake_adjust_worker_count.called)
        self.assertTrue(fake_check_workload.called)


class TestProcessLogs(unittest.TestCase):
    """A suite of tests for the entry point logic for the manager.py module"""
    @patch.object(manager, 'setproctitle')
    @patch.object(manager, 'KafkaConsumer')
    @patch.object(manager, 'adjust_worker_count')
    @patch.object(manager, 'produce_work')
    @patch.object(manager.time, 'sleep')
    @patch.object(manager, 'get_logger')
    def test_process_logs(self, fake_get_logger, fake_sleep, fake_produce_work,
                          fake_adjust_worker_count, fake_KafkaConsumer, fake_setproctitle):
        """``process_logs`` sets the process name to the supplied name"""
        fake_KafkaConsumer.return_value = ['eventA']
        fake_produce_work.side_effect = RuntimeError('stop running!')
        worker_cls = MagicMock()
        work_group = 'someGroup'
        topic = 'someTopic'
        server = 'myKafkaServer:9092'
        name = 'testing'

        manager.process_logs(worker_cls, work_group, topic, server, name)

        call_args, _ = fake_setproctitle.call_args
        proc_name = call_args[0]
        expected = name

        self.assertEqual(proc_name, expected)

    @patch.object(manager, 'setproctitle')
    @patch.object(manager, 'KafkaConsumer')
    @patch.object(manager, 'adjust_worker_count')
    @patch.object(manager, 'produce_work')
    @patch.object(manager.time, 'sleep')
    @patch.object(manager, 'get_logger')
    def test_process_logs_error(self, fake_get_logger, fake_sleep, fake_produce_work,
                          fake_adjust_worker_count, fake_KafkaConsumer, fake_setproctitle):
        """``process_logs`` sleeps upon total failure"""
        fake_KafkaConsumer.return_value = ['eventA']
        fake_produce_work.side_effect = Exception('stop running!')
        worker_cls = MagicMock()
        work_group = 'someGroup'
        topic = 'someTopic'
        server = 'myKafkaServer:9092'
        name = 'testing'

        manager.process_logs(worker_cls, work_group, topic, server, name)

        call_args, _ = fake_sleep.call_args
        slept_for = call_args[0]
        expected = 300

        self.assertEqual(slept_for, expected)


if __name__ == '__main__':
    unittest.main()
