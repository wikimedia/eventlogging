# -*- coding: utf-8 -*-
"""
  eventlogging unit tests
  ~~~~~~~~~~~~~~~~~~~~~~~

  This module contains test fixtures.

"""
from __future__ import unicode_literals

import copy
import signal

import eventlogging
import eventlogging.factory
from eventlogging.event import Event
import sqlalchemy


TEST_SCHEMA_SCID = ('TestSchema', 123)
TEST_META_SCHEMA_SCID = ('TestMetaSchema', 1)

# Schema for a meta subobject.
# TODO: Should this be somewhere more important than just in test fixtures.py?
#       e.g. in a json schema we can load with $ref?
_meta_properties = {
    'domain': {
        'type': 'string',
        'description': 'the domain the event pertains to'
    },
    'uri': {
        'type': 'string',
        'description': 'the unique URI identifying the event',
        'format': 'uri'
    },
    'topic': {
        'type': 'string'
    },
    'request_id': {
        'pattern': '^[a-fA-F0-9]{8}(-[a-fA-F0-9]{4}){3}-[a-fA-F0-9]{12}$',
        'type': 'string',
        'description': 'the unique UUID v1 ID of the event '
        'derived from the X-Request-Id header'
    },
    'dt': {
        'type': 'string',
        'description': 'the time stamp of the event, in ISO8601 format',
        'format': 'date-time'
    },
    'client_ip': {
        'type': 'string',
        'description': 'IP (possibly hashed) of the client '
        'submitting this event.'
    },
    'id': {
        'pattern': '^[a-fA-F0-9]{8}(-[a-fA-F0-9]{4}){3}-[a-fA-F0-9]{12}$',
        'type': 'string',
        'description': 'the unique ID of this event; should match the dt field'
    },
    'schema_uri': {
        'type': 'string',
        'description': 'The URI locating the schema for this event'
    }
}

_schemas = {
    eventlogging.schema.CAPSULE_SCID[0]: {
        eventlogging.schema.CAPSULE_SCID[1]: {
            'properties': {
                'clientIp': {
                    'type': 'string'
                },
                'event': {
                    'type': 'object',
                    'required': True
                },
                'wiki': {
                    'type': 'string',
                    'required': True
                },
                'webHost': {
                    'type': 'string'
                },
                'revision': {
                    'type': 'integer',
                    'required': True
                },
                'schema': {
                    'type': 'string',
                    'required': True
                },
                # TODO: Make change to EventCapsule on meta to include topic
                'topic': {
                    'type': 'string',
                },
                'recvFrom': {
                    'type': 'string',
                    'required': True
                },
                'seqId': {
                    'type': 'integer'
                },
                'timestamp': {
                    'type': 'number',
                    'required': True,
                    'format': 'utc-millisec'
                },
                'uuid': {
                    'type': 'string',
                    'required': True,
                    'format': 'uuid5-hex'
                },
                'userAgent': {
                    'type': 'string',
                    'description': 'User Agent from HTTP request',
                    'required': False
                }
            },
            'additionalProperties': False
        }
    },
    eventlogging.schema.ERROR_SCID[0]: {
        eventlogging.schema.ERROR_SCID[1]: {
            'properties': {
                'rawEvent': {
                    'type': 'string',
                    'required': True
                },
                'message': {
                    'type': 'string',
                    'required': True
                },
                'code': {
                    'type': 'string',
                    'required': True,
                    'enum': [
                        'processor',
                        'consumer',
                        'validation'
                    ],
                },
                'schema': {
                    'type': 'string',
                    'required': True
                },
                'revision': {
                    'type': 'integer',
                    'required': True
                }
            }
        }
    },
    TEST_SCHEMA_SCID[0]: {
        TEST_SCHEMA_SCID[1]: {
            'properties': {
                'value': {
                    'type': 'string',
                    'required': True
                },
                'nested': {
                    'type': 'object',
                    'properties': {
                        'deeplyNested': {
                            'type': 'object',
                            'properties': {
                                'pi': {
                                    'type': 'number',
                                }
                            }
                        }
                    }
                }
            }
        }
    },
    TEST_META_SCHEMA_SCID[0]: {
        TEST_META_SCHEMA_SCID[1]: {
            'description': 'Test Schema with Meta Data in Subobject',
            'title': 'TestMetaSchema',
            '$schema': 'http://json-schema.org/draft-04/schema#',
            'required': [
                'required_field',
            ],
            'type': 'object',
            'properties': {
                'required_field': {
                    'type': 'string',
                },
                'optional_field': {
                    'type': 'string',
                },
                'meta': {
                    'required': [
                        'topic',
                        'schema_uri',
                        'uri',
                        'request_id',
                        'id',
                        'dt',
                        'domain',
                        'client_ip'
                    ],
                    'type': 'object',
                    'properties': _meta_properties
                }
            }
        }
    }
}

_event = {
    'event': {
        'value': '☆ 彡',
        'nested': {
            'deeplyNested': {
                'pi': 3.14159
            }
        }
    },
    'seqId': 12345,
    'clientIp': '127.0.0.1',
    'timestamp': 1358791834912,
    'wiki': 'enwiki',
    'webHost': 'en.m.wikipedia.org',
    'recvFrom': 'fenari',
    'revision': 123,
    'schema': 'TestSchema',
    'uuid': 'babb66f34a0a5de3be0c6513088be33e',
    'topic': 'test_topic',
    'userAgent': 'test user agent'
}

# {} is preferred and PHP side of EL
# should be translating empty events to {} but this is
# to test that [] also works
_incorrectly_serialized_empty_event = {
    'event': [],
    'seqId': 12345,
    'clientIp': '127.0.0.1',
    'timestamp': 1358791834912,
    'wiki': 'enwiki',
    'webHost': 'en.m.wikipedia.org',
    'recvFrom': 'fenari',
    'revision': 123,
    'schema': 'TestSchema',
    'uuid': 'babb66f34a0a5de3be0c6513088be33e',
    'userAgent': 'test user agent'
}

# An event that doesn't use EventCapsule,
# Instead this uses meta subobject style metadata
_event_with_meta = {
    'meta': {
        'topic': 'topic_with_meta',
        'schema_uri': 'TestMetaSchema/1',
        'domain': 'en.m.wikipedia.org',
        'uri': 'http://en.m.wikipedia.org/nonya',
        'request_id': '12345678-1234-5678-1234-567812345678',
        'id': '12345678-1234-5678-1234-567812345678',
        'dt': '2015-10-30T00:00:00',
        'client_ip': '127.0.0.1',
    },
    'required_field': 'I am required',
    'optional_field': 'I am optional'
}


_topic_config = {
    'test_topic': {
        'schema_name': 'TestSchema',
        'revision': 123
    },
    'topic_with_meta': {
        'schema_name': 'TestMetaSchema'
    }
}


class HttpRequestAttempted(RuntimeError):
    """Raised on attempt to retrieve a schema via HTTP."""
    pass


# We'll be replacing :func:`eventlogging.schemas.url_get_schema` with a
# mock object, so set aside an unpatched copy so we can clean up.
orig_url_get_schema = eventlogging.schema.url_get_schema


def mock_url_get_schema(scid):
    """Mock of :func:`eventlogging.schemas.url_get_schema`
    Used to detect when :func:`eventlogging.schemas.get_schema`
    delegates to HTTP retrieval.
    """

    # Special case for test http get of non existent revision.
    # See test_service.py test_get_schema_unexisting_version
    if scid == ('TestMetaSchema', 1234):
        raise eventlogging.SchemaError('Mock HTTP SchemaError.')

    raise HttpRequestAttempted('Attempted HTTP fetch: %s' % (scid,))


def _get_event():
    """ Creates events on demand with unique ids"""
    for i in range(1, 100):
        event = Event(copy.deepcopy(_event))
        event['uuid'] = i
        yield event


class SchemaTestMixin(object):
    """A :class:`unittest.TestCase` mix-in for test cases that depend on
    schema look-ups."""

    def setUp(self):
        """Stub `url_get_schema` and pre-fill schema cache."""
        super(SchemaTestMixin, self).setUp()
        self.event = Event(copy.deepcopy(_event))
        self.event_with_meta = Event(copy.deepcopy(_event_with_meta))
        self.incorrectly_serialized_empty_event = Event(copy.deepcopy(
            _incorrectly_serialized_empty_event))
        eventlogging.schema.schema_cache = copy.deepcopy(_schemas)
        eventlogging.topic.topic_config = copy.deepcopy(_topic_config)
        eventlogging.schema.url_get_schema = mock_url_get_schema
        self.event_generator = _get_event()

    def tearDown(self):
        """Clear schema cache and restore stubbed `url_get_schema`."""
        super(SchemaTestMixin, self).tearDown()
        eventlogging.schema.schema_cache.clear()
        eventlogging.schema.url_get_schema = orig_url_get_schema

    def assertIsValid(self, event, msg=None):
        """Assert that capsule 'event' object validates."""
        return self.assertIsNone(eventlogging.validate(event), msg)

    def assertIsInvalid(self, event, msg=None):
        """Assert that capsule 'event' object fails validation."""
        with self.assertRaises(eventlogging.ValidationError, msg):
            eventlogging.validate(event)


class DatabaseTestMixin(SchemaTestMixin):
    """A :class:`unittest.TestCase` mix-in for database testing using an
    in-memory sqlite database."""

    def setUp(self):
        """Configure :class:`sqlalchemy.engine.Engine` and
        :class:`sqlalchemy.schema.MetaData` objects."""
        super(DatabaseTestMixin, self).setUp()
        self.engine = sqlalchemy.create_engine('sqlite://', echo=False)
        self.meta = sqlalchemy.MetaData(bind=self.engine)

    def tearDown(self):
        """Dispose of the database access objects."""
        super(DatabaseTestMixin, self).tearDown()
        self.meta.drop_all()
        self.engine.dispose()


class HttpSchemaTestMixin(object):
    """A :class:`unittest.TestCase` mix-in for stubbing HTTP responses."""

    http_resp = ''

    def setUp(self):
        """Replace `url_get` with stub."""
        super(HttpSchemaTestMixin, self).setUp()
        self.orig_url_get = eventlogging.schema.url_get
        eventlogging.schema.url_get = self.url_get_stub
        eventlogging.schema.schema_cache.clear()

    def tearDown(self):
        """Restore original `url_get`."""
        eventlogging.schema.url_get = self.orig_url_get

    def url_get_stub(self, url):
        """Test stub for `url_get`."""
        return self.http_resp


class HandlerTestMixin(object):
    def setUp(self):
        self.orig_writers = eventlogging.factory._writers.copy()
        eventlogging.factory._writers.clear()
        self.orig_readers = eventlogging.factory._readers.copy()
        eventlogging.factory._readers.clear()


class TimeoutTestMixin(object):
    """A :class:`unittest.TestCase` mix-in that imposes a time-limit on
    tests. Tests exceeding the limit are failed."""

    # Max time (in seconds) to allow tests to run before failing.
    max_time = 2

    def setUp(self):
        """Set the alarm."""
        super(TimeoutTestMixin, self).setUp()
        signal.signal(signal.SIGALRM, self.timeOut)
        signal.alarm(self.max_time)

    def tearDown(self):
        """Disable the alarm."""
        signal.alarm(0)

    def timeOut(self, signum, frame):
        """SIGALRM handler. Fails test if triggered."""
        self.fail('Timed out.')
