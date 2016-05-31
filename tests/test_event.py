# -*- coding: utf-8 -*-
"""
  eventlogging unit tests
  ~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for :module:`eventlogging.event`.
"""
from __future__ import unicode_literals

import copy
import json
import tempfile
import unittest

import eventlogging
from eventlogging import get_schema
from eventlogging.event import Event
from .fixtures import SchemaTestMixin, _event_with_meta


class EventTestCase(SchemaTestMixin, unittest.TestCase):
    def setUp(self):
        super(EventTestCase, self).setUp()

    def test_factory_dict(self):
        """Test Event.factory() with a dict."""
        self.assertEqual(
            Event.factory(_event_with_meta),
            _event_with_meta
        )

    def test_factory_list(self):
        """Test Event.factory() with a list of dicts."""
        l = [_event_with_meta, _event_with_meta]
        self.assertEqual(
            Event.factory(l),
            l
        )

    def test_factory_string(self):
        """Test Event.factory() with a JSON string."""
        s = json.dumps(_event_with_meta)
        self.assertEqual(
            Event.factory(s),
            _event_with_meta
        )

    def test_factory_string_list(self):
        """Test Event.factory() with a list JSON string."""
        l = [_event_with_meta, _event_with_meta]
        s = json.dumps(l)
        self.assertEqual(
            Event.factory(s),
            l
        )

    def test_factory_file(self):
        """Test Event.factory() with a file object."""
        l = [_event_with_meta, _event_with_meta]
        s = json.dumps(l)

        file = tempfile.TemporaryFile(prefix='eventlogging-event-test')
        # Write the string to the temp file.
        file.write(s.encode('utf-8'))
        # Seek to the beginning of file so Event.factory() can read it.
        file.seek(0)
        self.assertEqual(
            Event.factory(file),
            l
        )
        file.close()

    def test_serialize(self):
        """Test that serialize returns a JSON string."""
        s = self.event.serialize()
        # if we've got bytes, then decode as utf-8
        if type(s) == bytes:
            s = s.decode('utf-8')

        unserialized_event = json.loads(s)
        self.assertEqual(unserialized_event, self.event)

    def test_meta(self):
        """Test that meta() returns proper data for both styles of events."""
        # EventCapsule fields only
        event_capsule = copy.deepcopy(self.event)
        del event_capsule['event']

        # Test that the meta object returned from an encapsulated event
        # is the EventCapsule fields only.
        self.assertEqual(
            self.event.meta(),
            event_capsule
        )

        # Test that the meta object returned from a meta subobject
        # style event is just the meta object.
        self.assertEqual(
            self.event_with_meta.meta(),
            self.event_with_meta['meta']
        )

    def test_id(self):
        """Test that id/uuid can be extracted from event meta"""
        self.assertEqual(
            self.event.id(),
            'babb66f34a0a5de3be0c6513088be33e'
        )
        self.assertEqual(
            self.event_with_meta.id(),
            '12345678-1234-5678-1234-567812345678'
        )

    def test_schema_name(self):
        """Test that schema name can be extracted from event meta"""
        self.assertEqual(
            self.event.schema_name(),
            'TestSchema'
        )
        self.assertEqual(
            self.event_with_meta.schema_name(),
            'TestMetaSchema'
        )

    def test_schema_revision(self):
        """Test that schema revision can be extracted from event meta"""
        self.assertEqual(
            self.event.schema_revision(),
            123
        )
        self.assertEqual(
            self.event_with_meta.schema_revision(),
            1
        )

    def test_scid(self):
        """Test that scid can be extracted from an event meta"""
        self.assertEqual(
            self.event.scid(),
            ('TestSchema', 123)
        )
        self.assertEqual(
            self.event_with_meta.scid(),
            ('TestMetaSchema', 1)
        )

    def test_schema(self):
        """Test that schema can be extracted from an event meta"""
        self.assertEqual(
            self.event.schema(),
            eventlogging.schema.schema_cache['TestSchema'][123]
        )
        self.assertEqual(
            self.event.schema(encapsulate=True),
            get_schema(('TestSchema', 123), encapsulate=True)
        )
        self.assertEqual(
            self.event_with_meta.schema(),
            eventlogging.schema.schema_cache['TestMetaSchema'][1]
        )

    def test_datetime(self):
        """Test that a datetime can be extracted from event meta"""
        self.assertEqual(
            self.event.datetime(),
            eventlogging.utils.datetime_from_timestamp(self.event['timestamp'])
        )
        self.assertEqual(
            self.event_with_meta.datetime(),
            eventlogging.utils.datetime_from_timestamp(
                self.event_with_meta['meta']['dt']
            )
        )
        # No datetime to parse in event should return None by default
        del self.event_with_meta['meta']['dt']
        self.assertEqual(
            self.event_with_meta.datetime(),
            None
        )

    def test_topic(self):
        """Test that topic is extracted or interpolated from an event"""
        self.assertEqual(
            self.event.topic(topic_format='test_{schema}'),
            'test_' + self.event['schema']
        )

        self.assertEqual(
            self.event_with_meta.topic(),
            self.event_with_meta['meta']['topic']
        )
        self.assertEqual(
            self.event_with_meta.topic(topic_format='test_{meta[topic]}'),
            'test_' + self.event_with_meta['meta']['topic']
        )

        with self.assertRaises(eventlogging.topic.TopicNotFound):
            self.event_with_meta.topic(topic_format='test_{meta[not a key]}')
        with self.assertRaises(eventlogging.topic.TopicNotFound):
            del self.event_with_meta['meta']['topic']
            self.event_with_meta.topic()

    def test_has_meta_subobject(self):
        """Test that has_meta_subobject() returns appropriately"""
        self.assertFalse(self.event.has_meta_subobject())
        self.assertTrue(self.event_with_meta.has_meta_subobject())

    def test_should_encapsulate(self):
        """Test that should_encapsulate() returns appropriately"""
        self.assertTrue(self.event.should_encapsulate())
        self.assertFalse(self.event_with_meta.should_encapsulate())

    def test_set_scid(self):
        """
        Test that set_scid() sets schema,revision for capsule events,
        and schema_uri for meta subobject events.
        """
        fake_scid = ('A', 1)
        self.event.set_scid(fake_scid)
        self.assertEqual(
            (self.event['schema'], self.event['revision']), fake_scid
        )

        self.event_with_meta.set_scid(fake_scid)
        self.assertEqual(self.event_with_meta['meta']['schema_uri'], 'A/1')


class create_event_errorTestCase(object):
    """
    Tests for create_event_error function.
    """

    def test_create_event_error_unparsed(self):
        """
        create_event_error() should create a valid EventError
        object without schema or revision set.
        """

        invalid_raw_event = "Duh this won't validate against any schema."
        error_message = "This is just a test."
        error_code = "processor"
        event_error = eventlogging.create_event_error(
            invalid_raw_event,
            error_message,
            error_code
        )
        # Test that this event validates against the EventError schema.
        self.assertIsValid(event_error)
        self.assertEqual(
            event_error['schema'],
            eventlogging.schema.ERROR_SCID[0]
        )
        self.assertEqual(
            event_error['revision'],
            eventlogging.schema.ERROR_SCID[1]
        )
        self.assertEqual(
            event_error['event']['rawEvent'],
            invalid_raw_event
        )
        self.assertEqual(
            event_error['event']['message'],
            error_message
        )
        # assert that schema and revision are the defaults, since there's
        # there is no parsed_event and these are not even present in this
        # raw_event.
        self.assertEqual(
            event_error['event']['schema'],
            'unknown'
        )
        self.assertEqual(
            event_error['event']['revision'],
            -1
        )

    def test_create_event_error_parsed(self):
        """
        create_event_error() should create a valid EventError
        object with schema and revision set.
        """

        invalid_raw_event = "Duh this won't validate against any schema."
        error_message = "This is just a test."
        error_code = "processor"
        parsed_event = eventlogging.event.Event({
            'schema': 'Nonya',
            'revision': 12345
        })

        event_error = eventlogging.create_event_error(
            invalid_raw_event,
            error_message,
            error_code,
            parsed_event
        )
        # Test that this event validates against the EventError schema.
        self.assertIsValid(event_error)
        self.assertEqual(
            event_error['schema'],
            eventlogging.schema.ERROR_SCID[0]
        )
        self.assertEqual(
            event_error['revision'],
            eventlogging.schema.ERROR_SCID[1]
        )
        self.assertEqual(
            event_error['event']['rawEvent'],
            invalid_raw_event
        )
        self.assertEqual(
            event_error['event']['message'],
            error_message
        )
        # assert that schema and revision the same as in parsed_event
        self.assertEqual(
            event_error['event']['schema'],
            'Nonya'
        )
        self.assertEqual(
            event_error['event']['revision'],
            12345
        )

    def test_create_event_error_raw_schema_and_revision(self):
        """
        create_event_error() should create a valid
        EventError object with schema and revision set, extracted
        via a regex out of the raw_event.
        """

        invalid_raw_event = '?%7B%22event%22%3A%7B%22mobileMode%22%3A' \
            '%22stable%22%2C%22name%22%3A%22home%22%2C%22destination%22%3A' \
            '%22%2Fwiki%2FPagina_principale%22%7D%2C%22revision%22%3A' \
            '11568715%2C%22schema%22%3A' \
            '%22MobileWebMainMenuClickTracking%22%2C' \
            '%22webHost%22%3A%12345terfdit.m.wikipedia.org%22%2C%22wiki%22' \
            '%3A%22itwiki%22%7D;	cp3013.esams.wmnet	4724275	' \
            '2015-09-21T21:55:27	1.2.3.4	"Mozilla"'

        error_message = "This is just a test."
        error_code = "processor"

        event_error = eventlogging.create_event_error(
            invalid_raw_event,
            error_message,
            error_code,
        )
        # Test that this event validates against the EventError schema.
        self.assertIsValid(event_error)
        self.assertEqual(
            event_error['schema'],
            eventlogging.schema.ERROR_SCID[0]
        )
        self.assertEqual(
            event_error['revision'],
            eventlogging.schema.ERROR_SCID[1]
        )
        self.assertEqual(
            event_error['event']['rawEvent'],
            invalid_raw_event
        )
        self.assertEqual(
            event_error['event']['message'],
            error_message
        )
        # assert that schema and revision the same as in the invalid raw event
        self.assertEqual(
            event_error['event']['schema'],
            'MobileWebMainMenuClickTracking'
        )
        self.assertEqual(
            event_error['event']['revision'],
            11568715
        )
