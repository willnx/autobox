# -*- coding: UTF-8 -*-
"""Defines how to process logging events uploaded from user's gateways"""
from os import environ

import ujson
from cryptography.fernet import Fernet, InvalidToken

from log_processor.worker import Worker
from log_processor.influxdb import InfluxDB
from log_processor.manager import process_logs


class FirewallWorker(Worker):
    """Handles processing log data, then uploading it to InfluxDB

    Relies on the following environment variables:

        - INFLUXDB_SERVER: The IP/FQDN of the InfluxDB server
        - INFLUXDB_USER: The username to authenicate with
        - INFLUXDB_PASSWD_FILE: The location of the file with the user's password
        - CIPHER_KEY_FILE: The location of the file with the key to decrypt log data

    :param name: The general name of the work; i.e. firewall
    :type name: String

    :param id: The unique worker ID; gets appended to the name
    :type id: String/Integer

    :parm work_group: Gets used to identify the ``measurement`` to write to in InfluxDB
    :type work_group: String

    :param work_queue: The queue to pull work items from
    :type work_queue: multiprocessing.Queue

    :param idle_queue: Used to signal the manager that this worker is not doing anything.
    :type idle_queue: multiprocessing.Queue
    """
    def __init__(self, work_group, work_queue, idle_queue):
        super().__init__(work_group, work_queue, idle_queue)
        self.influx_server=environ['INFLUXDB_SERVER']
        self.influx_user = environ['INFLUXDB_USER']
        with open(environ['INFLUXDB_PASSWD_FILE'], 'rb') as pw_file:
            self.influx_password = pw_file.read().strip()
        with open(environ['CIPHER_KEY_FILE'], 'rb') as cipher_file:
            self.cipher_key = cipher_file.read().strip()
        self.cipher = None
        self.get_cipher()
        self.influx = None
        self.influx_get_conn()

    def get_cipher(self):
        self.cipher = Fernet(self.cipher_key)

    def extract(self, data):
        """Obtain the JSON object from the encrypted data"""
        return ujson.loads(self.cipher.decrypt(data))

    def influx_get_conn(self):
        """Create an network connection to the InfluxDB server"""
        self.influx = InfluxDB(server=self.influx_server,
                               user=self.influx_user,
                               password=self.influx_password,
                               measurement=self.work_group)

    def process_data(self, data):
        """Convert the data into a usable JSON object, then upload it to InfluxDB"""
        try:
            payload = self.extract(data)
        except (ValueError, InvalidToken) as doh:
            self.log.error('Error: {}, Data: {}'.format(doh, data))
        else:
            fields = {}
            tags = {}
            # writing strings to a field in Influx requires a double-quote
            tags['username'] = '"{}"'.format(payload['user'])
            # Dumbass Influx doesn't let you group by fields or aggregate tags...
            # I want to count the unique occurrences of a user over a period of time
            # to show current connected user counts, *and* be able to group by
            # those usernames over time to show specific user usage. Wish I
            # used TimescaleDB instead of InfluxDB
            fields['user'] = '"{}"'.format(payload.pop('user'))
            fields['source'] = '"{}"'.format(payload.pop('source'))
            fields['target'] = '"{}"'.format(payload.pop('target'))
            fields['packets'] = 1 # each event represents a single packet
            timestamp = payload.pop('time')
            self.influx.write(fields=fields, tags=tags, timestamp=timestamp)

    def flush_on_term(self):
        """Before termining, send all pending data points to InfluxDB"""
        self.influx.flush()

if __name__ == '__main__':
    process_logs(worker_cls=FirewallWorker,
                 topic='firewall',
                 server=environ['KAFKA_SERVER'],
                 work_group='firewall',
                 name='FirewallProcessor')
