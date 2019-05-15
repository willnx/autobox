#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from setuptools import setup, find_packages


setup(name="cronner",
      author="Nicholas Willhite,",
      author_email='willnx84@gmail.com',
      version='2019.05.16',
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
      description="A cron-like system for performing routine tasks",
      long_description=open('README.rst').read(),
      install_requires=['setproctitle', 'schedule', 'requests']
      )
