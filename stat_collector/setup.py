#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from setuptools import setup, find_packages


setup(name="stat-collector",
      author="Nicholas Willhite,",
      author_email='willnx84@gmail.com',
      version='2019.03.25',
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
      description="A system to process vLab logging data",
      long_description=open('README.rst').read(),
      install_requires=['ujson', 'cryptography', 'setproctitle', 'kafka-python', 'requests']
      )
