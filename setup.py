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

try: # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
    from pip.req import parse_requirements

# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_requirements = parse_requirements('requirements.txt', session=False)
# install_requires is a list of requirements
install_requires = [str(ir.req) for ir in install_requirements]

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
    test_suite='tests',
    install_requires=install_requires
)
