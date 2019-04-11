# -*- coding: UTF-8 -*-
"""Abstracts using the ElasticSearch API"""
import time

import requests


class ElasticSearch:
    """A usable connection to an ElasticSearch server

    :param server: The IP/FQDN of the ElasticSearch server
    :type server: String

    :param user: The user with write permissions on the ElasticSearch server
    :type user: String

    :param password: The user's password
    :type password: String

    :param doc_type: The name/category/type of document to interact with.
    :type doc_type: String

    :param port: The TCP port the elasticsearch server is listening on
    :type port: Integer

    :param verify: Check the TLS cert against the local CA trust. Default False
    :type verify: Boolean
    """
    def __init__(self, server, user, password, doc_type, port=9200, verify=False):
        self.url = 'https://{}:{}/{}/{}'
        self.server = server
        self.port = port
        self.creds = (user, password)
        self.session = requests.Session()
        self.doc_type = doc_type
        self.verify = verify

    @property
    def index(self):
        return time.strftime('logs-%Y.%m.%d')

    def write(self, document):
        """Add a new document to ElasticSearch.

        The supplied document MUST be formatted JSON, and that JSON MUST contain
        a key named ``timestamp`` with an EPOCH timestamp value of when the document
        was created. Failure to do this results in ElasitcSearch rejecting the write
        and/or breaking the analytics derived from the vLab logs.

        :Returns: None

        :Raises: requests.exceptions.HTTPError

        :param document: The new record/document to add to ElasticSearch
        :type document: JSON
        """
        url = self.url.format(self.server, self.port, self.index, self.doc_type)
        resp = self.session.post(url, auth=self.creds, data=document, headers={'Content-Type': 'application/json'}, verify=self.verify)
        resp.raise_for_status()

    def close(self):
        """TODO"""
        self.session.close()
