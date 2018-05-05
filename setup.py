#!/usr/bin/env python3

from distutils.core import setup

setup(name='Pypeline',
      version='1.0',
      description='Dataflow-inspired data pipeline in python 3',
      author='Antoine Carpentier',
      author_email='antoine.carpentier.info@gmail.com',
      url='https://github.com/t00n/pypeline',
      packages=['pypeline'],
      requires=['dateutil'])
