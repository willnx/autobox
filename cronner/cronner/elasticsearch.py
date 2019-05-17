# -*- coding: UTF-8 -*-
"""Functions to perform routine tasks for ElasticSearch"""
import time

import requests
requests.packages.urllib3.disable_warnings()

from cronner.std_logger import get_logger
from cronner.constants import const

logger = get_logger('elasticsearch')


def prune_indices(max_log_records=30):
    """Delete the oldest indices on the Elastic Search server.

    Elastic Search recommends deleting documents by removing the entire index.
    To accommodate that recommendation and limit amount of logs stored for the
    vLab Server, logs for a specific day have their own index. So keeping 30
    log records is equivalent to keeping 1 month of log data.

    :Returns: None

    :param max_log_records: The maximum number of indices to keep
    :type max_log_records: Integer
    """
    # A new index is created daily
    indices = _get_indices()
    to_prune = len(indices) - max_log_records
    logger.info('Total Indices: {}, Pruning: {}'.format(len(indices), to_prune))
    if to_prune > 0:
        logger.info('Pruning {} days worth of logs from elastic search'.format(to_prune))
        timestamp_pattern = 'logs-%Y.%m.%d'
        indices_map = {}
        # create a mapping of the epoch timestamp to the index name
        for index in indices:
            as_epoch = int(time.mktime(time.strptime(index, timestamp_pattern)))
            indices_map[as_epoch] = index
        # now, lets get a sorted list of the indexes; having them as an EPOCH
        # timestamp make sorting very simple
        indices_to_prune = []
        indices_keys = sorted(list(indices_map.keys()))
        for _ in range(to_prune):
            # as we pop off the oldest, add the index name to the list of
            # things we have to delete
            indices_to_prune.append(indices_map[indices_keys.pop(0)])
        # now create all the URLs to delete the oldest index
        for index_name in indices_to_prune:
            url = '{}:{}/{}'.format(const.ES_URL, const.ES_PORT, index_name)
            resp = _call_es(url, method='delete')
            if not resp.ok:
                msg = 'Failed to delete index {}, Status: {}, Msg: {}'.format(index_name, resp.status_code, resp.content)
                logger.error(msg)


def add_field_data():
    """Tell ElasticSearch to index the ``transaction_id`` attribute of the web logs

    This allows us to use the ``transaction_id`` as a 'drop down' in Grafana
    when looking a the vLab logs.

    :Returns: None

    :param indices: The name of the indices that exist on the Elastic Search server
    :type indices: Set
    """
    indices = _get_indices()
    payload = {"properties": {"transaction_id": {"type": "text", "fielddata": True}}}
    for index in indices:
        url = '{}:{}/{}/_mapping/web'.format(const.ES_URL, const.ES_PORT, index)
        resp = _call_es(url, method='put', json=payload)
        if not resp.ok:
            msg = 'Failed to update index {}, Status: {}, Msg: {}'.format(index, resp.status_code, resp.content.decode())
            logger.error(msg)


def _get_indices():
    """Obtain the set of indices that exist on the Elastic Search server

    :Returns: Set (of strings)
    """
    url = '{}:{}/_cat/indices'.format(const.ES_URL, const.ES_PORT)
    resp = _call_es(url)
    indices = set([])
    # example of a row
    # yellow open logs-2019.04.28 Ei_u-uXRTvyT3QJOYfWEpg 5 1 119376 0 53.6mb 53.6mb
    data = resp.content.decode().split('\n')
    for row in data:
        if row:
            index_name = row.split(' ')[2]
            indices.add(index_name)
    return indices


def _call_es(url, method='get', json=None):
    """Abstracts the SSL verification and Basic Auth aspect of talking to ElasitcSearch

    :Returns: requests.Response

    :param url: The URL to call
    :type url: String

    :param method: The HTTP method to invoke (i.e. GET/POST/DELETE/PUT)
    :type method: String

    :param json: The JSON payload to send with the requests
    :type json: PyObject
    """
    creds = (const.ES_USERNAME, const.ES_PASSWORD)
    caller = getattr(requests, method)
    resp = caller(url, verify=const.ES_SSL_VERIFY, auth=creds, json=json)
    return resp
