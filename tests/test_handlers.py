# -*- coding: utf-8 -*-
"""
  eventlogging unit tests
  ~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for :module:`eventlogging.handlers`.

"""


import os
import unittest
from mock import patch

import eventlogging
import eventlogging.handlers
import eventlogging.factory

from .fixtures import _get_event


def echo_writer(uri, **kwargs):
    values = []
    while 1:
        value = (yield values)
        values.append(value)


def repeater(uri, value, **kwargs):
    while 1:
        yield value


class HandlerFactoryTestCase(unittest.TestCase):
    """Test case for URI-based reader/writer factories."""

    def setUp(self):
        """Register test handlers."""
        eventlogging.writes('test')(echo_writer)
        eventlogging.reads('test')(repeater)

    def tearDown(self):
        """Unregister test handlers."""
        eventlogging.factory._writers.pop('test')
        eventlogging.factory._readers.pop('test')

    def test_get_writer(self):
        """``get_writer`` returns a scheme-appropriate primed coroutine."""
        writer = eventlogging.get_writer('test://localhost')
        self.assertEqual(writer.send(123), [123])

    def test_get_reader(self):
        """``get_reader`` returns the right generator for the URI scheme."""
        reader = eventlogging.get_reader('test://localhost/?value=secret')
        self.assertEqual(next(reader), 'secret')


class SQLHandlerTestCase(unittest.TestCase):

    def test_sql_batching_happy_case_same_schema(self):
        """
        Send several events that will get batched together
        as they belong to the same schema
        """

        # Patching works in the scope of the function that calls
        # it, until python3 doesn't play seameslly with unittest
        @patch('eventlogging.handlers.store_sql_events')
        def mock_holder(mock_store_sql_events):

            writer = eventlogging.get_writer(
                'sqlite://?batch_size=3&batch_time=100000')
            event = next(_get_event())

            writer.send(event)
            writer.send(event)
            self.assertEqual(
                mock_store_sql_events.call_count,
                0,
                'No call to insert should happened, batch size not reached'
            )
            # the two events belong to the same batch
            # the store_sql_events should have
            writer.send(event)
            self.assertEqual(
                mock_store_sql_events.call_count,
                1,
                'Reached batch size, should have inserted'
            )
            writer.send(event)
            self.assertEqual(
                mock_store_sql_events.call_count,
                1,
                'No call to insert should happened, batch size not reached'
            )

        mock_holder()

    def test_sql_batching_schemas_and_topics(self):
        """
        Send several events that will get batched separately
        as they belong to different topics but same schema
        """
        # Patching works in the scope of the function that calls
        # it, until python3 doesn't play seamlessly with unittest
        @patch('eventlogging.handlers.store_sql_events')
        def mock_holder(mock_store_sql_events):
            writer = eventlogging.get_writer(
                'sqlite://?batch_size=3&batch_time=100000')
            event_topic1 = next(_get_event())
            event_topic2 = next(_get_event())

            event_topic2['topic'] = 'different_test_topic'
            writer.send(event_topic1)
            writer.send(event_topic1)
            writer.send(event_topic2)
            writer.send(event_topic2)

            self.assertEqual(
                mock_store_sql_events.call_count,
                0,
                'No call to insert should happened, batch size not reached'
            )
            # now add a new event on second topic
            writer.send(event_topic2)
            self.assertEqual(
                mock_store_sql_events.call_count,
                1,
                'Reached batch size, should have inserted'
            )
            # add a new topic1 event
            writer.send(event_topic1)
            self.assertEqual(
                mock_store_sql_events.call_count,
                2,
                'Reached batch size, should have inserted'
            )

        mock_holder()


class PluginTestCase(unittest.TestCase):
    """Test case for the plug-in loader."""

    def setUp(self):
        """Determine path to mock plugin directory."""
        script_path = os.path.dirname(os.path.abspath(__file__))
        self.plugin_path = os.path.join(script_path, 'plugins')
        # clear the plugins that might have been loaded by other tests
        eventlogging.handlers.plugin_functions = {}

    def tearDown(self):
        # clear the plugins that might have been loaded by these tests
        eventlogging.handlers.plugin_functions = {}

    def test_load_plugins(self):
        """`get_plugins` loads plug-ins from an arbitrary path."""
        eventlogging.handlers.load_plugins(self.plugin_path)
        reader = eventlogging.get_reader('mock://localhost')
        self.assertEqual(next(reader), 'value generated by plug-in')
