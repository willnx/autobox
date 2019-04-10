# -*- coding: UTF-8 -*-
"""
Manages worker processes for converting raw log data into a well-structured
format that is uploaded to some database.

This framework will auto-scale the number of workers eagerly up, and slowly
down. The idea by doing this is:

  A) to better handle workflows that bursts supply records for processing
  B) Be less prone to flapping up/down scaling of workers
"""
import time
import queue
from os import environ
from multiprocessing import Queue, cpu_count

from kafka import KafkaConsumer
from setproctitle import setproctitle

from log_processor.std_logger import get_logger

PRODUCE_TIMEOUT = 30000 # milliseconds; how long to wait for a new work item from Kafka
PRODUCE_INTERVAL = 30 # seconds; minimum on how long to wait in between scaling workers
PRODUCE_BEFORE_CHECKING = 5000 # records; how many records to send to workers before checking on PRODUCE_INTERVAL
SENTINEL = 'TERMINATE YOU USELESS PROCESS'
SCALE_UP_BY = 2
SCALE_DOWN_BY = -1
MAX_WORKERS = 2 * cpu_count()


def make_queues():
    """Exists simply to make unit testing easier

    The ``work_queue`` is a standard queue, where each event pulled out of Kafka
    gets pushed into the queue.

    The ``idle_queue`` is a channel to enable workers to communicate directly back
    to the manager. Items in the ``idle_queue`` are tuples of ('process name', 'error').
    If no error occurred (like when we signal a worker to scale-down), the error-string
    will be of zero length (i.e. '').
    """
    work_queue = Queue()
    idle_queue = Queue()
    return work_queue, idle_queue


def check_workload(workers, work_queue, idle_queue, log):
    """Determine how much to scale up or down the number of worker processes.

    :Returns: Tuple (List, Int)

    :param workers: A list of active worker processes
    :type workers: List

    :param work_queue: The channel to dispatch work items/records to workers
    :type work_queue: multiprocessing.Queue

    :param idle_queue: The channel used by workers to communicate with the manager
    :type idle_queue: multiprocessing.Queue

    :param log: For writing log messages
    :type log: logging.Logger
    """
    log.debug('Checking worker health')
    needed_workers = 0
    workers, scale_modifier = check_worker_health(workers, idle_queue, log)
    needed_workers = 0
    work_in_queue = work_queue.qsize()
    log.debug('Pending items for processing: {}'.format(work_in_queue))
    log.debug('Active workers: {}'.format(len(workers)))
    if work_in_queue > 100:
        # Between adding PRODUCE_BEFORE_CHECKING to the work_queue, and performing
        # this check, over 100 work items remain to be processed. There's a
        # fair amount of logic to reach this point, so we should eagerly add
        # some more processes
        needed_workers = SCALE_UP_BY
    elif work_in_queue < 10:
        needed_workers = SCALE_DOWN_BY
    needed_workers = needed_workers + scale_modifier
    log.debug('Scaling workers by: {}'.format(needed_workers))
    return workers, needed_workers


def check_worker_health(workers, idle_queue, log):
    """Update the list of active workers, and identify if a we need to respawn
    a worker process.

    The tuple returns is a list of the current active workers, and an integer
    representing how to modify how much we scale the number of workers by.

    :Returns: Tuple (List, Int)

    :param workers: A list of active worker processes
    :type workers: List

    :param idle_queue: The channel used by workers to communicate with the manager
    :type idle_queue: multiprocessing.Queue

    :param log: For writing log messages
    :type log: logging.Logger
    """
    total_workers = len(workers)
    error_workers = 0
    scalier = 0
    terminated = set() # so we can clean up our list of active workers
    while not idle_queue.empty():
        try:
            worker, error = idle_queue.get(block=False)
            if error:
                error_workers += 1
            terminated.add(worker)
        except queue.Empty:
            break
    log.debug('Number of workers that encountered an error: {}'.format(error_workers))
    log.debug('Number of gracefully terminated workers: {}'.format(len(terminated) - error_workers))
    if error_workers == total_workers:
        raise RuntimeError('All workers are dead; aborting')
    elif error_workers != 0:
        new_workers = workers
        # Assumes that systemic errors will kill more works than we can create
        # such that we eventually end up with "error_workers == total_workers".
        # The scalier concept is an attempt to gracefully handle transient errors.
        scalier += 1
    # DO NOT use ``is_alive`` on workers to check if they are dead!
    # Using the ``idle_queue`` to check for terminated *and* failed workers
    # is the only way to avoid a race between discovering a worker gracefully
    # terminated, and accidentally scaling back up a purposefully terminated
    # worker.
    new_workers = []
    for worker in workers:
        if worker.name not in terminated:
            new_workers.append(worker)

    return new_workers, int(scalier)


def adjust_worker_count(workers, worker_cls, work_group,work_queue, idle_queue, need):
    """Scale up/down the number of workers

    :Returns: List

    :param workers: A list of active worker processes
    :type workers: List

    :param worker_cls: The specific Worker subclass for processing log data
    :type worker_cls: log_processor.worker.Worker

    :param work_queue: The channel to dispatch work items/records to workers
    :type work_queue: multiprocessing.Queue

    :param idle_queue: The channel used by workers to communicate with the manager
    :type idle_queue: multiprocessing.Queue

    :param need: The adjustment to make to the number of worker processes
    :type need: Integer
    """
    if need < 0 and len(workers) > 1:
        # having less than 1 worker is stupid
        work_queue.put(SENTINEL)
    else:
        potential_make = max(0, MAX_WORKERS - len(workers))
        to_make = min(need, potential_make)
        for _ in range(to_make):
            worker = worker_cls(work_group, work_queue, idle_queue)
            worker.start()
            workers.append(worker)
    return workers


def produce_work(workers, worker_cls, work_group, topic, work_queue, idle_queue, kafka, log):
    """Read records out of Kafka, and send them to the worker processes.

    :Returns: None

    :param workers: A list of active worker processes
    :type workers: List

    :param worker_cls: The specific Worker subclass for processing log data
    :type worker_cls: log_processor.worker.Worker

    :param topic: The Kafka topic to consume log data from
    :type topic: String

    :param work_queue: The channel to dispatch work items/records to workers
    :type work_queue: multiprocessing.Queue

    :param idle_queue: The channel used by workers to communicate with the manager
    :type idle_queue: multiprocessing.Queue

    :param kafka: The connection to Kafka for consuming records
    :type kafka: kafka.KafkaConsumer

    :param log: For writing log messages
    :type log: logging.Logger
    """
    produced = 0
    produce_start = time.time()
    for event in kafka:
        work_queue.put(event.value)
        produced += 1
        # incrementing a counter is more than x2 faster than checking a time delta.
        # PRODUCE_BEFORE_CHECKING should be large enough produce enough work
        # to keep a single worker busy while we check if we need more works.
        # A single user connecting to a webpage in their lab will produce
        # over 600 records, so keep that in mind when adjusting PRODUCE_BEFORE_CHECKING
        if produced >= PRODUCE_BEFORE_CHECKING:
            produced = 0
            log.debug('produced {} items, checking time'.format(PRODUCE_BEFORE_CHECKING))
            loop_delta = time.time() - produce_start
            if loop_delta > PRODUCE_INTERVAL:
                log.debug('Checking work workload')
                workers, need = check_workload(workers, work_queue, idle_queue, log)
                workers = adjust_worker_count(workers, worker_cls, work_group, work_queue, idle_queue, need)
                produce_start = time.time()


def process_logs(worker_cls, work_group, topic, server, name):
    """Entry point for running a log processor

    :Returns: None

    :param worker_cls: The specific Worker subclass for processing log data
    :type worker_cls: log_processor.worker.Worker

    :param work_group: Generally, this is the name of the table to use on the
                       target database (even if it's not literally a table).
    :type work_group: String

    :param topic: The Kafka topic to consume log data from
    :type topic: String

    :param server: The IP/FDQN and port of the Kafka server (ex - 1.2.3.4:9092)
    :type server: String

    :param name: The name of this specific log processor
    :type name: String
    """
    setproctitle(name)
    log = get_logger(name)
    log.info('Starting manager for {}'.format(name))
    log.info('Using worker: {}'.format(worker_cls))
    log.info('Max produce timeout: {} milliseconds'.format(PRODUCE_TIMEOUT))
    log.info('Max produce interval: {} seconds'.format(PRODUCE_INTERVAL))
    log.info('Max records before check: {}'.format(PRODUCE_BEFORE_CHECKING))
    log.info('Max number of workers allowed: {}'.format(MAX_WORKERS))
    log.info('Kafka server: {}'.format(server))
    log.info('Kafka topic: {}'.format(topic))
    kafka = KafkaConsumer(topic, bootstrap_servers=server, consumer_timeout_ms=PRODUCE_TIMEOUT)
    workers = []
    work_queue, idle_queue = make_queues()
    adjust_worker_count(workers, worker_cls, work_group, work_queue, idle_queue, need=1)
    while True:
        try:
            produce_work(workers, worker_cls, work_group, topic, work_queue, idle_queue, kafka, log)
        except Exception as doh:
            log.execption(doh)
            log.error('Sleeping to prevent flapping and to make data gaps (so a human will notice a problem)')
            time.sleep(300)
            break
