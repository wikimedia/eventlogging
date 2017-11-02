# -*- coding: utf-8 -*-
"""
  eventlogging unit tests
  ~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for :module:`eventlogging.schema`.

  This does not test JSON Schema validation per se, since validation
  is delegated to  :module:`jsonschema`, which comes with its own
  comprehensive suite of unit tests. This module specifically tests
  schema cache / HTTP retrieval and the handling of complex
  ('encapsulated') event objects.

"""
from __future__ import unicode_literals

import unittest

import eventlogging

from .fixtures import (
    HttpRequestAttempted,
    HttpSchemaTestMixin,
    SchemaTestMixin,
    TEST_SCHEMA_SCID
)


class HttpSchemaTestCase(HttpSchemaTestMixin, unittest.TestCase):
    """Tests for :func:`eventlogging.schema.url_get_schema`."""

    def test_valid_resp(self):
        """Test handling of HTTP response containing valid schema."""
        self.http_resp = '{"properties":{"value":{"type":"number"}}}'
        schema = eventlogging.schema.url_get_schema(TEST_SCHEMA_SCID)
        self.assertEqual(schema, {'properties': {'value': {'type': 'number'}}})

    def test_invalid_resp(self):
        """Test handling of HTTP response not containing valid schema."""
        self.http_resp = '"foo"'
        with self.assertRaises(eventlogging.SchemaError):
            eventlogging.schema.url_get_schema(TEST_SCHEMA_SCID)

    def test_caching(self):
        """Valid HTTP responses containing JSON Schema are cached."""
        self.http_resp = '{"properties":{"value":{"type":"number"}}}'
        eventlogging.get_schema(TEST_SCHEMA_SCID)
        self.assertIn(TEST_SCHEMA_SCID[0], eventlogging.schema.schema_cache)
        self.assertIn(
            TEST_SCHEMA_SCID[1],
            eventlogging.schema.schema_cache[TEST_SCHEMA_SCID[0]]
        )


class FileSchemaTestCase(unittest.TestCase):
    def test_scid_from_uri(self):
        """Tests that an scid can be extracted from filenames / URIs."""
        self.assertEqual(
            eventlogging.schema.scid_from_uri('FakeSchema/123.json'),
            ('FakeSchema', 123)
        )
        self.assertEqual(
            eventlogging.schema.scid_from_uri('FakeSchema/123.yaml'),
            ('FakeSchema', 123)
        )
        self.assertEqual(
            eventlogging.schema.scid_from_uri(
                '/whatever/path/to/FakeSchema/123.yaml'
            ),
            ('whatever/path/to/FakeSchema', 123)
        )
        self.assertEqual(
            eventlogging.schema.scid_from_uri(
                '/whatever/path/to/FakeSchema/123.yaml',
                '/whatever/path/to',
            ),
            ('FakeSchema', 123)
        )
        self.assertEqual(
            eventlogging.schema.scid_from_uri(
                'file:///whatever/path/to/FakeSchema/123.yaml'
            ),
            ('whatever/path/to/FakeSchema', 123)
        )
        self.assertEqual(
            eventlogging.schema.scid_from_uri(
                'file:///whatever/path/to/FakeSchema/123.yaml',
                '/whatever/path'
            ),
            ('to/FakeSchema', 123)
        )
        self.assertEqual(
            eventlogging.schema.scid_from_uri(
                'http://example.org/whatever/path/to/FakeSchema/123.yaml',
                '/whatever/path'
            ),
            ('to/FakeSchema', 123)
        )
        self.assertEqual(
            eventlogging.schema.scid_from_uri('12345 not a schema file'),
            None
        )
        self.assertEqual(
            eventlogging.schema.scid_from_uri('FakeSchema/123'),
            ('FakeSchema', 123)
        )
        self.assertEqual(
            eventlogging.schema.scid_from_uri('FakeSchema'),
            ('FakeSchema', None)
        )


class ValidateScidTestCase(unittest.TestCase):
    """Tests for :func:`eventlogging.schema.validate_scid`."""

    schema_name, revision_id = TEST_SCHEMA_SCID

    def test_valid_scid(self):
        """Valid SCIDs validate."""
        scid = self.schema_name, self.revision_id
        self.assertIsNone(eventlogging.schema.validate_scid(scid))

    def test_invalid_schema_name(self):
        """Invalid schema name triggers SCID validation failure."""
        for invalid_schema_name in ('Foo%', 'X' * 64, 123):
            scid = invalid_schema_name, self.revision_id
            with self.assertRaises(eventlogging.ValidationError):
                eventlogging.schema.validate_scid(scid)

    def test_invalid_revision_id(self):
        """Invalid revision ID triggers SCID validation failure."""
        for invalid_revision_id in (-1, '1'):
            scid = self.schema_name, invalid_revision_id
            with self.assertRaises(eventlogging.ValidationError):
                eventlogging.schema.validate_scid(scid)

            with self.assertRaises(eventlogging.ValidationError):
                eventlogging.schema.validate_scid('not a tuple')


class SchemaTestCase(SchemaTestMixin, unittest.TestCase):
    """Tests for :module:`eventlogging.schema`."""

    def test_valid_event(self):
        """Valid events validate."""
        self.assertIsValid(self.event)

    def test_incomplete_scid(self):
        """Missing SCID in capsule object triggers validation failure."""
        self.event.pop('schema')
        self.assertIsInvalid(self.event)

    def test_missing_property(self):
        """Missing property in capsule object triggers validation failure."""
        self.event.pop('uuid')
        self.assertIsInvalid(self.event)

    def test_missing_nested_property(self):
        """Missing property in nested event triggers validation failure."""
        self.event['event'].pop('value')
        self.assertIsInvalid(self.event)

    def test_extra_property(self):
        """Missing property in nested event triggers validation failure."""
        self.event['event']['season'] = 'summer'
        self.assertIsInvalid(self.event)

    def test_schema_retrieval(self):
        """Schemas missing from the cache are retrieved via HTTP."""
        # Pop the schema from the cache.
        eventlogging.schema.schema_cache.pop(TEST_SCHEMA_SCID[0])
        with self.assertRaises(HttpRequestAttempted) as context:
            eventlogging.validate(self.event)
            self.assertEqual(context.exception.rev_id, 123)

    def test_encapsulated_schema(self):
        """get_schema() returns encapsulated schema if requested."""
        encapsulated = eventlogging.get_schema(eventlogging.CAPSULE_SCID)
        encapsulated['event'] = eventlogging.get_schema(TEST_SCHEMA_SCID)
        self.assertEqual(eventlogging.get_schema(TEST_SCHEMA_SCID, True),
                         encapsulated)

    def test_capsule_uuid(self):
        """capsule_uuid() generates a unique UUID for capsule objects."""
        self.assertEqual(eventlogging.capsule_uuid(self.event),
                         '93e0f58d0c605e90a3c4861b1f00c407')

    def test_empty_event(self):
        """An empty event with no mandatory properties should validate"""
        self.assertIsValid(self.incorrectly_serialized_empty_event)

    def test_init_schema_cache(self):
        """
        Test that non file based schemas are removed from schema_cache
        after call to init_schema_cache().
        """
        self.assertTrue(
            TEST_SCHEMA_SCID[0] in eventlogging.schema.schema_cache
        )
        eventlogging.schema.init_schema_cache()
        self.assertFalse(
            TEST_SCHEMA_SCID[0] in eventlogging.schema.schema_cache
        )

    def test_cache_schema(self):
        """
        Test that a schema exists in schema_cache after call to cache_schema().
        """
        scid = ('Dummy', 1)
        schema = 'dummy'

        eventlogging.schema.cache_schema(scid, schema)
        self.assertEqual(
            eventlogging.schema.schema_cache[scid[0]][scid[1]],
            schema
        )

    def test_get_latest_schema_revision(self):
        """
        Test that get_latest_schema_revision() always returns the
        max schema revision for a schema, or None if no schema exists.
        """
        self.assertEqual(
            eventlogging.schema.get_latest_schema_revision('TestSchema'),
            123
        )
        scid = ('TestSchema', 999999)
        schema = 'dummy'
        # cache a new dummy schema larger than the current TestSchema revision
        eventlogging.schema.cache_schema(scid, schema)
        self.assertEqual(
            eventlogging.schema.get_latest_schema_revision('TestSchema'),
            scid[1]
        )
        # this schema name doesn't exist, so should return none.
        self.assertEqual(
            eventlogging.schema.get_latest_schema_revision('nopers'),
            None
        )

    def test_url_from_scid(self):
        f = 'https://meta.wikimedia.org/w/api.php?action=jsonschema' \
            '&title=%s&revid=%s&formatversion=2'
        scid = ('TestSchema', 123)
        self.assertEqual(
            eventlogging.schema.url_from_scid(scid),
            f % (scid)
        )

    def test_scid_from_uri(self):
        scid = TEST_SCHEMA_SCID
        self.assertEqual(
            eventlogging.schema.scid_from_uri('%s/%s' % scid),
            scid
        )
        self.assertEqual(
            eventlogging.schema.scid_from_uri('%s/%s.json' % scid),
            scid
        )
        self.assertEqual(
            eventlogging.schema.scid_from_uri('%s/%s.yaml' % scid),
            scid
        )
        self.assertEqual(
            eventlogging.schema.scid_from_uri('%s/%s.yml' % scid),
            scid
        )
        self.assertEqual(
            eventlogging.schema.scid_from_uri('http://d.org/%s/%s' % scid),
            scid
        )
        self.assertEqual(
            eventlogging.schema.scid_from_uri(
                'file:///a/b/%s/%s' % scid,
                '/a/b'
            ),
            scid
        )
        # Test that we can get a scid with the latest schema revision
        # if the schema_uri doesn't have a revision, and we set
        # default_to_latest_revision to True.
        self.assertEqual(
            eventlogging.schema.scid_from_uri(
                scid[0], default_to_latest_revision=True
            ),
            scid
        )
        # Test that we can get a scid with a revision of None
        # if the schema_uri doesn't have a revision, and we set
        # default_to_latest_revision to False.
        self.assertEqual(
            eventlogging.schema.scid_from_uri(
                scid[0]
            ),
            (scid[0], None)
        )

    def test_is_schema_cached(self):
        self.assertTrue(eventlogging.schema.is_schema_cached(
            ('TestSchema', 123)
        ))
        self.assertFalse(eventlogging.schema.is_schema_cached(
            ('TestSchema', 999)
        ))
        self.assertFalse(eventlogging.schema.is_schema_cached(
            ('NopeNope', 123)
        ))

    def test_schema_uri_from_scid(self):
        scid = TEST_SCHEMA_SCID
        self.assertEqual(
            eventlogging.schema.schema_uri_from_scid(scid),
            '%s/%s' % scid
        )

    def test_get_cached_scids(self):
        scid = TEST_SCHEMA_SCID
        self.assertTrue(scid in eventlogging.schema.get_cached_scids())

    def test_cached_schema_uris(self):
        scid = TEST_SCHEMA_SCID
        uri = '%s/%s' % scid
        self.assertTrue(uri in eventlogging.schema.get_cached_schema_uris())
