#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from setuptools import setup, find_packages


setup(name="log-exporter",
      author="Nicholas Willhite,",
      author_email='willnx84@gmail.com',
      version='2019.04.08',
      packages=find_packages(),
      include_package_data=True,
      classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.6',
      ],
      description="Exports vLab logging data",
      long_description=open('README.rst').read(),
      install_requires=['ujson', 'cryptography', 'setproctitle', 'kafka-python', 'docker']
      )
