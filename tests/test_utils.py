# -*- coding: utf-8 -*-
"""
  eventlogging unit tests
  ~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for :module:`eventlogging.utils`.

"""


import datetime
import dateutil.parser
from dateutil.tz import tzutc
import unittest
import uuid

import eventlogging
from eventlogging.compat import long


class FlattenUnflattenTestCase(unittest.TestCase):
    """Test cases for :func:`eventlogging.utils.flatten` and
    :func:`eventlogging.utils.unflatten`."""

    deep = {'k1': 'v1', 'k2': 'v2', 'k3': {'k3a': {'k3b': 'v3b'}}}
    flat = {'k1': 'v1', 'k2': 'v2', 'k3_k3a_k3b': 'v3b'}

    def test_flatten(self):
        """``flatten`` flattens a dictionary with nested dictionary values."""
        flattened = eventlogging.utils.flatten(self.deep)
        self.assertEqual(flattened, self.flat)

    def test_unflatten(self):
        """``unflatten`` makes a flattened dictionary deep again."""
        unflattened = eventlogging.utils.unflatten(self.flat)
        self.assertEqual(unflattened, self.deep)

    def test_flatten_unflatten_inverses(self):
        """``flatten`` and ``unflatten`` are inverse functions."""
        self.assertEqual(eventlogging.utils.flatten(
            eventlogging.utils.unflatten(self.flat)), self.flat)
        self.assertEqual(eventlogging.utils.unflatten(
            eventlogging.utils.flatten(self.deep)), self.deep)


class UtilsTestCase(unittest.TestCase):
    """Test case for :module:`eventlogging.utils`."""

    def test_uri_delete_query_item(self):
        """``uri_delete_query_item`` deletes a query item from a URL."""
        uri = 'http://www.com?aa=aa&bb=bb&cc=cc'
        test_data = (
            ('aa', 'http://www.com?bb=bb&cc=cc'),
            ('bb', 'http://www.com?aa=aa&cc=cc'),
            ('cc', 'http://www.com?aa=aa&bb=bb'),
        )
        for key, expected_uri in test_data:
            actual_uri = eventlogging.uri_delete_query_item(uri, key)
            self.assertEqual(actual_uri, expected_uri)

    def test_update_recursive(self):
        """``update_recursive`` updates a dictionary recursively."""
        target = {'k1': {'k2': {'k3': 'v3'}}}
        source = {'k1': {'k2': {'k4': 'v4'}}}
        result = {'k1': {'k2': {'k3': 'v3', 'k4': 'v4'}}}
        eventlogging.utils.update_recursive(target, source)
        self.assertEqual(target, result)

    def test_is_subset_dict(self):
        """``is_subset_dict`` can tell whether a dictionary is a subset
        of another dictionary."""
        map = {'k1': {'k2': 'v2', 'k3': 'v3'}, 'k4': 'v4'}
        subset = {'k1': {'k3': 'v3'}}
        not_subset = {'k1': {'k4': 'v4'}}
        self.assertTrue(eventlogging.utils.is_subset_dict(subset, map))
        self.assertFalse(eventlogging.utils.is_subset_dict(not_subset, map))

    def test_parse_etcd_uri(self):
        """`parse_etcd_uri` returns proper kwargs from uri"""
        etcd_uri = 'https://hostA:123,hostB:234?' \
                   'cert=/path/to/cert&allow_redirect=True'

        etcd_kwargs = eventlogging.utils.parse_etcd_uri(etcd_uri)
        expected_kwargs = {
            'protocol': 'https',
            'host': (('hostA', 123), ('hostB', 234)),
            'cert': '/path/to/cert',
            'allow_redirect': True
        }
        for key in list(expected_kwargs.keys()):
            self.assertEqual(etcd_kwargs[key], expected_kwargs[key])

    def test_datetime_from_uuid1(self):
        """`test_datetime_from_uuid1` returns correct datetime"""
        u = uuid.uuid1()
        ts = ((u.time - long(0x01b21dd213814000))*100/1e9)
        self.assertEqual(
            eventlogging.utils.datetime_from_uuid1(u),
            datetime.datetime.fromtimestamp(ts)
        )

    def test_datetime_from_timestamp(self):
        """`datetime_from_timestamp` returns correct datetime"""
        ts = 1447270770
        ts_milli = 1447270770.00000
        dt = datetime.datetime.fromtimestamp(ts).replace(tzinfo=tzutc())
        iso8601 = dt.isoformat()
        self.assertEqual(
            eventlogging.utils.datetime_from_timestamp(ts),
            dt
        )
        self.assertEqual(
            eventlogging.utils.datetime_from_timestamp(ts_milli),
            dt
        )
        self.assertEqual(
            eventlogging.utils.datetime_from_timestamp(iso8601),
            dt
        )
        with self.assertRaises(RuntimeError):
            eventlogging.utils.datetime_from_timestamp(self)

    def test_timestamp_from_datetime(self):
        """`timestamp_from_datetime` returns correct timestamps"""
        dt = dateutil.parser.parse('2018-06-01T12:24:12Z')
        ts = 1527855852
        self.assertEqual(
            eventlogging.utils.timestamp_from_datetime(dt, milliseconds=False),
            ts
        )
        self.assertEqual(
            eventlogging.utils.timestamp_from_datetime(dt, milliseconds=True),
            int(ts * 1000)
        )

    def test_kafka_ids(self):
        """
        Tests that kafka_ids returns (client_id, group_id) based on identity.
        """
        (client_id, group_id) = eventlogging.utils.kafka_ids()
        self.assertTrue(
            client_id.startswith('eventlogging-'),
            'client_id should start with eventlogging-'
        )
        self.assertTrue(
            group_id.startswith('eventlogging-'),
            'group_id should start with eventlogging-'
        )

        (client_id, group_id) = eventlogging.utils.kafka_ids('test')
        self.assertTrue(
            client_id.startswith('test-'),
            'client_id should start with test-'
        )
        self.assertEqual(
            group_id,
            'test',
            'group_id should equal test'
        )

    def test_ua_parse_ios(self):
        ios_ua = 'WikipediaApp/5.3.3.1038 (iOS 10.2; Phone)'
        parsed = {
            'os_minor': '2',
            'os_major': '10',
            'device_family': 'Other',
            'os_family': 'iOS',
            'browser_major': None,
            'browser_minor': None,
            'browser_family': 'Other',
            'wmf_app_version': '5.3.3.1038',
            'is_bot': False,
            'is_mediawiki': False
        }
        self.assertEqual(parsed,
                         eventlogging.utils.parse_ua(ios_ua))

    def test_ua_parse_android(self):
        android_ua = 'WikipediaApp/2.4.160-r-2016-10-14 (Android 4.4.2; Phone)'
        parsed = {
            'os_major': '4',
            'wmf_app_version': '2.4.160-r-2016-10-14',
            'os_family': 'Android',
            'device_family': 'Generic Smartphone',
            'browser_family': 'Android',
            'browser_minor': '4',
            'browser_major': '4',
            'os_minor': '4',
            'is_bot': False,
            'is_mediawiki': False
        }
        self.assertEqual(parsed,
                         eventlogging.utils.parse_ua(android_ua))

    def test_ua_parse_empty(self):
        ua = ""
        parsed = {
            'os_minor': None,
            'os_major': None,
            'device_family': 'Other',
            'os_family': 'Other',
            'browser_major': None,
            'browser_minor': None,
            'browser_family': 'Other',
            'wmf_app_version': '-',
            'is_bot': False,
            'is_mediawiki': False
        }
        self.assertEqual(parsed,
                         eventlogging.utils.parse_ua(ua))

    def test_ua_parse_mediawiki(self):
        mw_ua = 'MediaWiki 1.28'
        parsed = {
            'os_major': None,
            'wmf_app_version': '-',
            'os_family': 'Other',
            'device_family': 'Generic Feature Phone',
            'browser_family': 'Other',
            'browser_minor': None,
            'browser_major': None,
            'os_minor': None,
            'is_bot': False,
            'is_mediawiki': True
        }
        self.assertEqual(parsed,
                         eventlogging.utils.parse_ua(mw_ua))

    def test_ua_parse_max_length(self):
        long_ua = ("Mozilla/5.0 (X11; Linux x86_64_128) looooonguaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        with self.assertRaises(RuntimeError):
            eventlogging.utils.parse_ua(long_ua)
