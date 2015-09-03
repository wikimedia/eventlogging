# -*- coding: utf-8 -*-
"""
  eventlogging unit tests
  ~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for :module:`eventlogging.topic`.

"""
from __future__ import unicode_literals

import unittest

import eventlogging

from .fixtures import (
    SchemaTestMixin,
    TEST_SCHEMA_SCID
)


class TopicTestCase(SchemaTestMixin, unittest.TestCase):

    def test_scid_for_topic(self):
        """Test that the configured scid is returned for a topic"""
        self.assertEqual(
            eventlogging.topic.scid_for_topic('use_latest_revision'),
            (TEST_SCHEMA_SCID[0], 0)
        )
        self.assertEqual(
            eventlogging.topic.scid_for_topic('use_specified_revision'),
            TEST_SCHEMA_SCID
        )
        with self.assertRaises(Exception):
            eventlogging.topic.scid_for_topic('not a real topic')

    def test_schema_for_topic(self):
        """Test that the configured schema is returned for a topic"""
        self.assertEqual(
            eventlogging.topic.schema_for_topic('use_specified_revision'),
            eventlogging.schema.schema_cache[TEST_SCHEMA_SCID[0]][
                TEST_SCHEMA_SCID[1]
            ]
        )

    def test_topic_from_event(self):
        """Test that topic is extracted or interpolated from an event"""
        self.assertEqual(
            eventlogging.topic.topic_from_event(
                self.event,
                topic_format='test_{schema}'
            ),
            'test_' + self.event['schema']
        )

        self.assertEqual(
            eventlogging.topic.topic_from_event(self.event_with_meta),
            self.event_with_meta['meta']['topic']
        )
        self.assertEqual(
            eventlogging.topic.topic_from_event(
                self.event_with_meta,
                topic_format='test_{meta[topic]}'
            ),
            'test_' + self.event_with_meta['meta']['topic']
        )
        with self.assertRaises(eventlogging.topic.TopicNotFound):
            eventlogging.topic.topic_from_event(
                self.event_with_meta,
                topic_format='test_{meta[not a key]}'
            )
        with self.assertRaises(eventlogging.topic.TopicNotFound):
            del self.event_with_meta['meta']['topic']
            eventlogging.topic.topic_from_event(self.event_with_meta)
