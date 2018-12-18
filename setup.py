#!/usr/bin/env python

from setuptools import find_packages
from distutils.core import setup
import os.path
import re
import sys

try:
    from semantic_release import setup_hook
    setup_hook(sys.argv)
except ImportError:
    pass


def read(filename):
    return open(os.path.join(os.path.dirname(__file__), filename)).read()


version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                    read('kafka_event_hub/__init__.py'), re.MULTILINE).group(1)

if sys.argv[-1] == 'publish':
    os.system("python setup.py sdist")
    os.system('twine upload dist/swissbib_kafka_event_hub-{}.tar.gz -r pypi'.format(version))
    sys.exit()


description = 'Swissbib Kafka Event Hub'

long_description = read('README.md')


install_require = ['confluent-kafka', 'PyYAML', 'Sickle', 'PyMongo', 'requests', 'simple-elastic']

setup(
    name='swissbib_kafka_event_hub',
    packages=find_packages(),
    description=description,
    long_description=long_description,
    version=version,
    author='Jonas Waeber',
    author_email='jonas.waeber@unibas.ch',
    url='https://github.com/swissbib/kafka_event_hub',
    keywords=['kafka', 'swissbib', 'oai', 'mongodb'],
    install_requires=install_require,
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Software Development :: Libraries :: Python Modules"
        ],
    license='LICENSE'
    )
