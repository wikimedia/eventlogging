# -*- coding: utf-8 -*-
"""
  eventlogging unit tests
  ~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for :class:`eventlogging.LogParser`.

"""
from __future__ import unicode_literals

import calendar
import datetime
import unittest

import eventlogging


class NcsaTimestampTestCase(unittest.TestCase):
    """Test case for converting to or from NCSA Common Log format."""

    def test_ncsa_timestamp_handling(self):
        epoch_ts = calendar.timegm(datetime.datetime.utcnow().utctimetuple())
        ncsa_ts = eventlogging.ncsa_utcnow()
        self.assertAlmostEqual(eventlogging.ncsa_to_unix(ncsa_ts),
                               epoch_ts, delta=100)


class LogParserTestCase(unittest.TestCase):
    """Test case for LogParser."""

    maxDiff = None

    def test_parse_client_side_events(self):
        """Parser test: client-side events."""
        parser = eventlogging.LogParser(
            '%q %{recvFrom}s %{seqId}d %D %o %u')
        raw = ('?%7B%22wiki%22%3A%22testwiki%22%2C%22schema%22%3A%22Generic'
               '%22%2C%22revision%22%3A13%2C%22event%22%3A%7B%22articleId%2'
               '2%3A1%2C%22articleTitle%22%3A%22H%C3%A9ctor%20Elizondo%22%7'
               'D%2C%22webHost%22%3A%22test.wikipedia.org%22%7D; srv0.example.org '
               '132073 2013-01-19T23:16:38 - '
               'Mozilla/5.0 (X11; Linux x86_64; rv:10.0)'
               ' Gecko/20100101 Firefox/10.0')
        ua = {
                'os_minor': None,
                'os_major': None,
                'device_family': 'Other',
                'os_family': 'Linux',
                'browser_major': '10',
                'browser_minor': '0',
                'browser_family': 'Firefox',
                'wmf_app_version': '-',
                'is_bot': False,
                'is_mediawiki': False
            }
        parsed = {
            'uuid': 'e6b17e1f661155bd8523d14c87d0600b',
            'recvFrom': 'srv0.example.org',
            'wiki': 'testwiki',
            'webHost': 'test.wikipedia.org',
            'seqId': 132073,
            'dt': '2013-01-19T23:16:38',
            'schema': 'Generic',
            'revision': 13,
            'userAgent': ua,
            'event': {
                'articleTitle': 'Héctor Elizondo',
                'articleId': 1
            }
        }
        fromParser = parser.parse(raw)
        for key in parsed:
            if key == 'userAgent':
                self.assertEqual(parsed[key],
                                 fromParser[key])
            else:
                self.assertEqual(fromParser[key], parsed[key])



    def test_parse_capsule_user_agent(self):
        """
        Parser test: client-side events with userAgent in submitted capsule.
        If a capsule has a field that is also parsed from the raw event line,
        the capsule's field should be preferred.
        """
        parser = eventlogging.LogParser(
            '%q %{recvFrom}s %{seqId}d %D %o %u')
        raw = ('?%7B%22wiki%22%3A%22testwiki%22%2C%22schema%22%3A%22Generic'
               '%22%2C%22revision%22%3A13%2C%22event%22%3A%7B%22articleId%2'
               '2%3A1%2C%22articleTitle%22%3A%22H%C3%A9ctor%20Elizondo%22%7'
               'D%2C%22webHost%22%3A%22test.wikipedia.org%22%2C'
               '%22userAgent%22%3A%22Mozilla%2F5.0%5Cu0020%28Windows%5C'
               'u0020NT%5Cu00206.1%3B%5Cu0020WOW64%29%5Cu0020AppleWebKit'
               '%2F537.36%5Cu0020%28KHTML%2C%5Cu0020like%5Cu0020Gecko%29'
               '%5Cu0020Chrome%2F61.0.3163.100%5Cu0020Safari%2F537.36%22'
               '%7D; srv0.example.org 132073 2013-01-19T23:16:38 - '
               'Mozilla/5.0 (X11; Linux x86_64; rv:10.0)'
               ' Gecko/20100101 Firefox/10.0')
        # This ua is the parsed userAgent from inside the event capsule, NOT
        # the last field in the log line.
        ua = {
            'browser_family': 'Chrome',
            'browser_major': '61',
            'browser_minor': '0',
            'device_family': 'Other',
            'is_bot': False,
            'is_mediawiki': False,
            'os_family': 'Windows 7',
            'os_major': None,
            'os_minor': None,
            'wmf_app_version': '-'
        }
        parsed = {
            'uuid': 'e6b17e1f661155bd8523d14c87d0600b',
            'recvFrom': 'srv0.example.org',
            'wiki': 'testwiki',
            'webHost': 'test.wikipedia.org',
            'seqId': 132073,
            'dt': '2013-01-19T23:16:38',
            'schema': 'Generic',
            'revision': 13,
            'userAgent': ua,
            'event': {
                'articleTitle': 'Héctor Elizondo',
                'articleId': 1
            }
        }
        fromParser = parser.parse(raw)
        for key in parsed:
            if key == 'userAgent':
                self.assertEqual(parsed[key],
                                 fromParser[key])
            else:
                self.assertEqual(fromParser[key], parsed[key])

    def test_parser_bot_requests(self):
        parser = eventlogging.LogParser(
            '%q %{recvFrom}s %{seqId}d %D %o %u')
        # Bot - recognised by uaparser
        raw = ('?%7B%22wiki%22%3A%22testwiki%22%2C%22schema%22%3A%22Generic'
               '%22%2C%22revision%22%3A13%2C%22event%22%3A%7B%22articleId%2'
               '2%3A1%2C%22articleTitle%22%3A%22H%C3%A9ctor%20Elizondo%22%7'
               'D%2C%22webHost%22%3A%22test.wikipedia.org%22%7D; cp3022.esa'
               'ms.wikimedia.org 132073 2013-01-19T23:16:38 - '
               'AppEngine-Google; (+http://code.google.com/appengine; appid'
               ': webetrex)')
        ua_map = parser.parse(raw)['userAgent']
        self.assertEqual(ua_map['is_bot'], True)
        # Bot - not recognised by uaparser
        raw = ('?%7B%22wiki%22%3A%22testwiki%22%2C%22schema%22%3A%22G'
               'eneric%22%2C%22revision%22%3A13%2C%22event%22%3A%7B%22artic'
               'leId%22%3A1%2C%22articleTitle%22%3A%22H%C3%A9ctor%20Elizond'
               'o%22%7D%2C%22webHost%22%3A%22test.wikipedia.org%22%7D; cp30'
               '22.esams.wikimedia.org 132073 2013-01-19T23:16:38 - '
               'WikiDemo/10.2.0;')
        ua_map = parser.parse(raw)['userAgent']
        self.assertEqual(ua_map['is_bot'], True)
        # Regular browser
        raw = ('?%7B%22wiki%22%3A%22testwiki%22%2C%22schema%22%3A%22'
               'Generic%22%2C%22revision%22%3A13%2C%22event%22%3A%7B%22arti'
               'cleId%22%3A1%2C%22articleTitle%22%3A%22H%C3%A9ctor%20Elizon'
               'do%22%7D%2C%22webHost%22%3A%22test.wikipedia.org%22%7D; cp3'
               '022.esams.wikimedia.org 132073 2013-01-19T23:16:38 - '
               'Mozilla/5.0 (X11; Linux x86_64; rv:10.0)'
               ' Gecko/20100101 Firefox/10.0')
        ua_map = parser.parse(raw)['userAgent']
        self.assertEqual(ua_map['is_bot'], False)


    def test_parse_failure(self):
        """Parse failure raises ValueError exception."""
        parser = eventlogging.LogParser('%q %{recvFrom}s %t')
        with self.assertRaises(ValueError):
            parser.parse('Fails to parse.')


    def test_repr(self):
        """Calling 'repr' on LogParser returns canonical string
        representation."""
        parser = eventlogging.LogParser('%q %{seqId}d %t')
        self.assertEqual(repr(parser), "<LogParser('%q %{seqId}d %t')>")
