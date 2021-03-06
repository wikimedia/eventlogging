# -*- coding: utf-8 -*-
"""
  eventlogging unit tests
  ~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for :module:`eventlogging.topic`.

"""


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

    def test_update_topic_config(self):
        """
        Test updating an individual topic config to the global topic config.
        """
        test_topic_config = {
            'test.topic': {
                'schema_name': 'test.schema'
            }
        }
        local_topic_config = eventlogging.topic.get_topic_config()
        local_topic_config.update(test_topic_config)

        # append the new test topic config to the global topic config
        eventlogging.topic.update_topic_config(test_topic_config)

        # test that the global topic config is what it should be
        self.assertEqual(
            eventlogging.topic.get_topic_config(),
            local_topic_config
        )

    def test_regex_schema_lookup(self):
        """
        Test the topic config lookup by regex
        """
        test_topic_config = {
            '/^regex\.*/': {
                'schema_name': 'regex.schema'
            }
        }

        eventlogging.topic.update_topic_config(test_topic_config)

        self.assertEqual(
            eventlogging.topic.schema_name_for_topic('regex.topic'),
            'regex.schema'
        )
        self.assertTrue(eventlogging.topic.is_topic_configured('regex.topic'))
        self.assertFalse(eventlogging.topic.is_topic_configured('bla'))
