# -*- coding: utf-8 -*-
"""
  eventlogging unit tests
  ~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for :module:`eventlogging.topic`.

"""
from __future__ import unicode_literals

import unittest

import eventlogging

from .fixtures import SchemaTestMixin


class TopicTestCase(SchemaTestMixin, unittest.TestCase):

    def test_get_topic_config(self):
        self.assertEqual(
            eventlogging.topic.get_topic_config(),
            eventlogging.topic.topic_config
        )

    def test_schema_name_for_topic(self):
        """Test that the configured schema name is returned for a topic"""
        self.assertEqual(
            eventlogging.topic.schema_name_for_topic('test_topic'),
            'TestSchema'
        )
        with self.assertRaises(eventlogging.topic.TopicNotConfigured):
            eventlogging.topic.schema_name_for_topic('not a schema')

    def test_latest_scid_for_topic(self):
        self.assertEqual(
            eventlogging.topic.latest_scid_for_topic('test_topic'),
            ('TestSchema', 123)
        )

    def test_schema_allowed_in_topic(self):
        self.assertTrue(
            eventlogging.topic.schema_allowed_in_topic(
                'TestSchema', 'test_topic'
            )
        )
        self.assertFalse(
            eventlogging.topic.schema_allowed_in_topic(
                'not a schema', 'test_topic'
            )
        )
        with self.assertRaises(eventlogging.topic.TopicNotConfigured):
            eventlogging.topic.schema_allowed_in_topic(
                'not a schema', 'not a topic'
            )
