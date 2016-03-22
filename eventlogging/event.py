# -*- coding: utf-8 -*-
"""
  eventlogging.event
  ~~~~~~~~~~~~~~~~~~~

  This module contains an Event class that wraps a dict and provides
  helper functions for transparently accessing metadata.
"""
from __future__ import unicode_literals

import re
import socket
import time
import uuid

from .compat import json, string_types
from .schema import (
    get_schema, CAPSULE_SCID, ERROR_SCID, url_from_scid, scid_from_uri,
    SCHEMA_RE_PATTERN
)
from .topic import TopicNotFound
from .utils import datetime_from_timestamp

__all__ = ('create_event_error', 'Event')


class Event(dict):
    """
    Event is a wrapper around a Python dict.  It exists
    in order to abstract away the details of the location
    of meta data for this event.  Currently, EventLogging
    supports two types of event meta data:

    - EventCapsule
    - meta-subobject

    This class adds helper functions to a normal dict to allow
    you to work with Event meta data without worrying about
    the implementation details about where this metadata is stored.
    Aside from the set_scid() function, none of these functions
    change any state of the dict.
    """

    @staticmethod
    def factory(data):
        """
        Given an open file, JSON string, dict, or list of dicts,
        this function will convert it to the corresponding
        Event(s).  That is, a JSON string dict or a dict
        will be returned as an Event, and a JSON string list
        or a list will be assumed to be a list of dicts
        and will be returned as a list of Events.  If a file
        object is given, its contents will be read.  The contents
        are assumed to be valid JSON.
        """

        # NOTE that data is transformed in each of the following steps.

        # 1. Try to convert from an object with a read method.
        try:
            data = data.read()
        except (AttributeError, TypeError):
            pass

        # 2. Try to convert from bytes to utf-8 string.
        try:
            data = data.decode('utf-8')
        except (AttributeError, TypeError):
            pass

        # 3. Try to convert from a JSON string to a python object.
        if isinstance(data, string_types):
            data = json.loads(data)

        # 4. If the python object is a list, convert each
        #    entry to an Event object.
        if isinstance(data, list):
            return list(map(Event, data))
        # 5. Else it was just an object, convert it to an Event.
        else:
            return Event(data)

    def __repr__(self):
        return '<Event {0} of schema {1}>'.format(self.id(), self.scid())

    def schema(self, encapsulate=False):
        """
        Returns the jsonschema for this event.
        """
        return get_schema(
            self.scid(),
            encapsulate=encapsulate
        )

    def schema_uri(self):
        """
        If this is a meta subobject style event, then this
        returns schema_uri from metadata.  Else a url will be
        constructed using schema.url_from_scid().
        """
        if self.has_meta_subobject():
            return self['meta']['schema_uri']
        else:
            return url_from_scid(self.scid())

    def topic(self, topic_format=None, default=None):
        """
        Returns the topic for this event.

        This will return the topic to use for this event.
        If topic_format is None, then the value of 'topic' in the event
        meta data will be used.
        Otherwise the event wil be formatted with topic_format.
        E.g.
          topic_format = 'eventlogging_{schema}'
            OR
          topic_format = 'eventlogging_{meta[schema]}'

          topic = topic_format.format(**event)

        :raises :exc:`TopicNotFound` if topic cannot be extracted from event
        """
        topic = default
        if not topic_format:
            if 'topic' not in self.meta():
                raise TopicNotFound(
                    'Could not extract topic from event meta data.'
                )
            else:
                topic = self.meta()['topic']
        else:
            # Interpolate topic from self
            try:
                topic = topic_format.format(**self)
            except KeyError as e:
                raise TopicNotFound(
                    'Could not interpolate topic from event with format '
                    '\'%s\'. KeyError: %s' % (topic_format, e))

        return topic

    def meta(self):
        """
        Returns the meta data for this event.
        """
        if self.has_meta_subobject():
            meta = self.get('meta')
        # else use keys from EventCapsule as meta
        else:
            capsule_keys = get_schema(CAPSULE_SCID)['properties'].keys()
            # don't include 'event' in capsule keys, it is the event data.
            meta = {
                key: self.get(key) for key in capsule_keys if (
                    key != 'event' and key in self
                )
            }
        return meta

    def id(self):
        """
        Returns the uuid id from meta data.
        """
        # Look for 'id' or 'uuid' in event meta data.
        return self.meta().get(
            'id',
            self.meta().get('uuid')
        )

    def datetime(self):
        """
        Looks for a timestamp in meta data,
        parses this into a datetime.datetime object
        and returns it.
        """
        t = self.meta().get(
            'dt',
            self.meta().get('timestamp')
        )
        if not t:
            return None
        else:
            return datetime_from_timestamp(t)

    def schema_name(self):
        """
        Returns the schema name for this event.
        """
        return self.scid()[0]

    def schema_revision(self):
        """
        Returns the schema revision for this event.
        """
        return self.scid()[1]

    def scid(self):
        """
        Gets an scid out of meta data.
        """
        scid = None
        if self.has_meta_subobject():
            if 'schema_uri' in self['meta']:
                scid = scid_from_uri(self['meta']['schema_uri'])
        elif 'schema' in self and 'revision' in self:
            scid = (self['schema'], self['revision'])

        return scid

    def has_meta_subobject(self):
        """
        Returns true if this event uses meta subobject style
        metadata rather than being an EventCapsule event.
        """
        return 'meta' in self and isinstance(self['meta'], dict)

    def should_encapsulate(self):
        """
        By default, events use EventCapsule style metadata, unless they
        have a 'meta' object in them.
        """
        # Return true if meta is not present, or if meta is not a dict.
        return not self.has_meta_subobject()

    def set_scid(self, scid):
        """
        Sets the scid for this event.
        - schema_uri if meta subject style
        - schema and revision if EventCapsule style
        """
        if self.has_meta_subobject():
            # Set schema uri to a simple name/revision uri.
            self['meta']['schema_uri'] = scid[0] + '/' + str(scid[1])
        else:
            self['schema'] = scid[0]
            self['revision'] = scid[1]


def create_event_error(
    raw_event,
    error_message,
    error_code,
    parsed_event=None
):
    """
    Creates an EventError around this raw_event string.
    If parsed_event is provided, The raw event's schema and revision
    will be included in the ErrorEvent as event.schema and event.revision.
    Otherwise these will be attempted to be extracted from the raw_event via
    a regex.  If this still fails, these will be set to 'unknown' and -1.
    """
    # These REs will be used when constructing an ErrorEvent
    # to extract the schema and revision out of a raw event
    # string in the case it cannot be parsed as JSON.
    RAW_SCHEMA_RE = re.compile(
        r'%22schema%22%3A%22({0})%22'.format(SCHEMA_RE_PATTERN)
    )
    RAW_REVISION_RE = re.compile(r'%22revision%22%3A(\d+)')

    errored_schema = 'unknown'
    errored_revision = -1

    # If we've got a parsed event, then we can just get the schema
    # and revision out of the object.
    if parsed_event:
        scid = parsed_event.scid()
        if scid:
            errored_schema, errored_revision = scid

    # otherwise attempt to get them out of the raw_event with a regex
    else:
        schema_match = RAW_SCHEMA_RE.search(raw_event)
        if schema_match:
            errored_schema = schema_match.group(1)

        revision_match = RAW_REVISION_RE.search(raw_event)
        if revision_match:
            errored_revision = int(revision_match.group(1))

    return Event({
        'schema': ERROR_SCID[0],
        'revision': ERROR_SCID[1],
        'wiki': '',
        'uuid': '%032x' % uuid.uuid1().int,
        'recvFrom': socket.getfqdn(),
        'timestamp': int(round(time.time())),
        'event': {
            'rawEvent': raw_event,
            'message': error_message,
            'code': error_code,
            'schema': errored_schema,
            'revision': errored_revision
        }
    })
