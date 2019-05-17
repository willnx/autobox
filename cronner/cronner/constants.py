# -*- coding: UTF-8 -*-
"""
All the things can override via Environment variables are keep in this one file.

.. note::
    Any and all values that *are* passwords must contain the string 'AUTH' in
    the name of the constant. This is how we avoid logging passwords.
"""
from os import environ
from collections import namedtuple, OrderedDict


DEFINED = OrderedDict([
            ('ES_USERNAME', environ.get('ES_USERNAME', 'someUser')),
            ('ES_PASSWORD', environ.get('ES_PASSWORD', 'IloveKats!')),
            ('ES_URL', environ.get('ES_URL', 'https://elasticsearch.org')),
            ('ES_PORT', int(environ.get('ES_PORT', 9200))),
            ('ES_SSL_VERIFY', bool(environ.get('ES_SSL_VERIFY', False))),
          ])

Constants = namedtuple('Constants', list(DEFINED.keys()))

# The '*' expands the list, just liked passing a function *args
const = Constants(*list(DEFINED.values()))
