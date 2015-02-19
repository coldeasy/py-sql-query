#!/usr/bin/env python

import os
import sys

import sqlquery

from codecs import open

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

packages = [
    'sqlquery',
]

requires = []

with open('README.rst', 'r', 'utf-8') as f:
    readme = f.read()

setup(
    name='sqlquery',
    version=sqlquery.__version__,
    description='SQL query translation.',
    long_description=readme,
    author='Colin Deasy',
    author_email='coldeasy@gmail.com',
    url='https://github.com/coldeasy/py-sql-query',
    packages=packages,
    package_data={'': ['LICENSE']},
    package_dir={'sqlquery': 'sqlquery'},
    include_package_data=True,
    install_requires=requires,
    license='Apache 2.0',
    zip_safe=True,
    classifiers=(
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ),
)
