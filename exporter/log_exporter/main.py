# -*- coding: UTF-8 -*-
"""Take the logs from a running Docker container and send them to Kafka for processing"""
import os
import time
import logging
import threading

import ujson
import docker
from kafka import KafkaProducer
from cryptography.fernet import Fernet
from setproctitle import setproctitle

LOOP_INTERVAL = 10
CIPHER_KEY_FILE = '/etc/vlab/log_sender.key'


class Exporter(threading.Thread):
    """Follows a log file and sends the information to Kafka log server"""
    def __init__(self, container, topic, server, log, cipher_file=CIPHER_KEY_FILE, retries=5):
        super(Exporter, self).__init__()
        self.log = log
        self.container = container
        self.topic = topic
        self.server = server
        self.conn = self._get_kafka_conn(self.server, retries)
        self.cipher = self._get_cipher(cipher_file)

    def _get_kafka_conn(self, server, retries):
        conn = KafkaProducer(bootstrap_servers=server, retries=retries)
        return conn

    def _get_cipher(self, cipher_file):
        with open(cipher_file, 'rb') as the_file:
            key = the_file.read().strip()
        cipher = Fernet(key)
        return cipher

    def run(self):
        """This method is evoked after calling ``start``"""
        try:
            lines = []
            for line in self.container.logs(follow=True, stream=True, since=int(time.time())):
                if line.startswith(b' ') or len(lines) == 0:
                    lines.append(line)
                    continue
                else:
                    payload = {'name' : self.container.name, 'log' : b''.join(lines)}
                    message = self.cipher.encrypt(ujson.dumps(payload).encode())
                    self.conn.send(self.topic, message)
                    lines = [line]
        except Exception as doh:
            self.log.exception(doh)
        finally:
            self.log.info('Exporter for {} terminating'.format(self.container.name))
            self.conn.close()


def respawn_workers(client, workers, kafka_server, log):
    """Compaire the active exporter threads with active docker containers, and
    re-create any exporter threads as needed.

    While running, Docker containers will start/stop. When a running container
    terminates, the associated log exporter thread also terminates. If Docker
    re-creates the container, we need to spawn a new exporter thread.

    :Returns: Dictionary

    :param client: The docker client
    :type client: docker.client.DockerClient

    :param workers: A mapping of container names to active log exporter threads
    :type workers: Dictionary

    :param kafka_server: The <address:port> of the Kafka server to upload logs to
    :type kafka_server: String

    :param log: An object for logging (yo dawg)
    :type log: logging.Logger
    """
    alive = {x.name: x for x in client.containers.list()}
    for container_name, worker in workers.items():
        if not worker.is_alive():
            alive_container = alive.get(container_name, None)
            if alive_container:
                topic = get_topic(container_name)
                respawned = Exporter(container=alive_container,
                                     topic=topic,
                                     server=kafka_server,
                                     log=log)
                respawned.start()
                workers[container_name] = respawned
                del worker
            else:
                log.info('No alive container for {}'.format(container_name))
    return workers


def spawn_workers(client, workers, kafka_server, log):
    """Creates a log exporting thread for each active container

    :Returns: Dictionary

    :param client: The docker client
    :type client: docker.client.DockerClient

    :param workers: A mapping of container names to active log exporter threads
    :type workers: Dictionary

    :param kafka_server: The <address:port> of the Kafka server to upload logs to
    :type kafka_server: String

    :param log: An object for logging (yo dawg)
    :type log: logging.Logger
    """
    for container in client.containers.list():
        active_exporter = workers.get(container.name, None)
        if not active_exporter:
            log.info('Spawning exporter for: {}'.format(container.name))
            topic = get_topic(container.name)
            new_worker = Exporter(container=container,
                                  topic=topic,
                                  server=kafka_server,
                                  log=log)
            new_worker.start()
            workers[container.name] = new_worker
    return workers


def get_topic(container_name):
    """Obtain the correct Kafka topic to use when forwarding logs

    :Returns: String

    :param container_name: The name of the container creating the logs
    :type container_name: String
    """
    # We want to push Apache-style logs, Celery worker logs, and others to
    # unique topics because it really simplifies the parser logic when processing
    # the logs
    # Example of container names
    # vlab_insightiq-api_1       <- Apache-style
    # vlab_insightiq-worker_1    <- Celery worker
    # vlab_dns_1                 <- DNS-specific log
    log_group = container_name.split('_')[-2]
    try:
        log_style = log_group.split('-')[1]
    except IndexError:
        log_style = log_group

    topic = ""
    if log_style == 'api':
        topic = 'web'
    elif log_style == 'worker':
        topic = 'worker'
    elif log_style == 'dns':
        topic = 'dns'
    elif log_style == 'ntp':
        topic = 'ntp'
    else:
        topic = 'other'
    return topic


def get_logger(name, loglevel='INFO'):
    """Simple factory function for creating logging objects

    :Returns: logging.Logger

    :param name: The name of the logger (typically just __name__).
    :type name: String

    :param loglevel: The verbosity of the logging; ERROR, INFO, DEBUG
    :type loglevel: String
    """
    logger = logging.getLogger(name)
    logger.setLevel(loglevel.upper())
    if not logger.handlers:
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(message)s')
        ch.setLevel(loglevel.upper())
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger


def main():
    """Entry point logic"""
    log = get_logger('log_exporter')
    client = docker.from_env()
    kafka_server = os.environ.get('KAFKA_SERVER', '127.0.0.1:9092')
    log.info('vLab log Exporter starting')
    log.info('Docker info:\n{}'.format(client.version()))
    workers = {}
    while True:
        loop_start = time.time()
        workers = spawn_workers(client, workers, kafka_server, log)
        workers = respawn_workers(client, workers, kafka_server, log)
        spawn_time = max(0, time.time() - loop_start)
        delta = LOOP_INTERVAL - spawn_time
        sleep_for = max(0, delta)
        time.sleep(sleep_for)


if __name__ == '__main__':
    setproctitle('Log Exporter')
    main()
