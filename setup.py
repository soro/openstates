#!/usr/bin/env python
from setuptools import setup

long_description = open('README.rst').read()

setup(name='openstates',
      version='0.1',
      packages=['billy', 'billy.scrape', 'billy.importers',
                'billy.bin', 'billy.misc', 'billy.site',
                'billy.site.browse', 'billy.site.api'],
      description='The Open State Project',
      long_description=long_description,
      platforms=['any'],
      )
