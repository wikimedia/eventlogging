"""
eventlogging
~~~~~~~~~~~~

This module contains scripts for processing streams of events generated
by `EventLogging`_, a MediaWiki extension for logging structured data.

.. _EventLogging: https://www.mediawiki.org/wiki/Extension:EventLogging

"""
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

# Workaround for <https://bugs.python.org/issue15881#msg170215>:
import multiprocessing  # noqa


setup(
    name='eventlogging',
    version='0.9',
    license='GPL',
    author='Ori Livneh',
    author_email='ori@wikimedia.org',
    url='https://www.mediawiki.org/wiki/Extension:EventLogging',
    description='Server-side component of EventLogging MediaWiki extension.',
    long_description=__doc__,
    classifiers=(
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: '
            'GNU General Public License v2 or later (GPLv2+)',
        'Programming Language :: JavaScript',
        'Programming Language :: PHP',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Topic :: Database',
        'Topic :: Scientific/Engineering :: '
            'Interface Engine/Protocol Translator',
        'Topic :: Software Development :: Object Brokering',
    ),
    packages=(
        'eventlogging',
        'eventlogging.lib',
    ),
    scripts=(
        'bin/eventlogging-forwarder',
        'bin/eventlogging-multiplexer',
        'bin/eventlogging-consumer',
        'bin/eventlogging-devserver',
        'bin/eventlogging-processor',
        'bin/eventlogging-reporter',
    ),
    zip_safe=False,
    test_suite='eventlogging.tests',
    install_requires=(
        "python-dateutil>=1.5",
        "jsonschema>=0.7",
        "kafka-python>=0.9.4",
        # To run eventlogging with python3 in debian, you'll need a at
        # python-mysqldb >= 1.3.4, which is not available in Jessie.
        # pip calls this library 'mysqlclient', so we specify that here.
        # NOTE:  This is commented out until we no longer run on Jessie
        # and we no longer use sudo python setup.py install to install.
        # "mysqlclient>=1.3.7",
        "pygments>=1.5",
        "pykafka>=2.1.0",
        # python-etcd requires python-openssl >= 0.14, which is not
        # available in Trusty.  Our python-etcd package does work with
        # python-openssl 0.13-2 which is available via .deb.
        # Commenting out this python dependency and allowing puppet
        # to satisify it until we upgrade eventlogging servers to Jessie.
        # "python-etcd>=0.4.0",
        "PyYAML>=3.10",
        "pyzmq>=2.1",
        "sqlalchemy>=0.7",
        "statsd>=3.0",
        # NOTE:  This is commented out until we no longer run on Jessie
        # and we no longer use sudo python setup.py install to install.
        # "tornado>=4.0",
        # "sprockets.mixins.statsd>=1.3.1",
    )
)
