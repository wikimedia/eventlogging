# -*- coding: utf-8 -*-
"""
  eventlogging.capsule
  ~~~~~~~~~~~~~~~~~~~~

  This module implements the event capsule schema.
  Since January 2018 the event capsule is hardcoded in this file
  belonging to EventLogging's codebase, as opposed to be stored
  in https://meta.wikimedia.org/wiki/Schema:EventCapsule.
  The reason is it's highly coupled with how EventLogging
  works internally and it doesn't make sense to have it
  publicly editable as if it was a generic schema.
  See: https://phabricator.wikimedia.org/T179836.
  This schema is now documented in: https://wikitech.wikimedia.org/
    wiki/Analytics/Systems/EventLogging/EventCapsule

"""
import copy

__all__ = ('get_event_capsule_schema',)

EVENT_CAPSULE_SCHEMA = {
    'description':
        'A wrapper around event objects that encodes generic metadata',
    'properties': {
        'event': {
            'type': 'object',
            'description': 'The encapsulated event object',
            'required': True
        },
        'wiki': {
            'type': 'string',
            'description': "$wgDBName (for example: 'enwiki')",
            'required': True
        },
        'webHost': {
            'type': 'string',
            'description': ("Request host. 'window.location.hostname' "
                            "on client-side events; $_SERVER['HTTP_HOST'] "
                            'on server.')
        },
        'schema': {
            'type': 'string',
            'description': 'Title of event schema',
            'required': True
        },
        'revision': {
            'type': 'integer',
            'description': 'Revision ID of event schema',
            'required': True
        },
        'topic': {
            'type': 'string',
            'description': 'The queue topic name this event belongs in'
        },
        'recvFrom': {
            'type': 'string',
            'description': 'Hostname of server emitting the log line',
            'required': True
        },
        'timestamp': {
            'type': 'number',
            'description': ('UTC unix epoch timestamp of event. '
                            'Optional. Should be the same as dt. '
                            'Exists for backwards compatibility. See: '
                            'https://phabricator.wikimedia.org/T179625')
        },
        'dt': {
            'type': 'string',
            'description': 'UTC ISO-8601 timestamp of event',
            'format': 'date-time'
        },
        'seqId': {
            'type': 'integer',
            'description': 'Udp2log sequence ID'
        },
        'uuid': {
            'type': 'string',
            'description': 'Unique event identifier',
            'format': 'uuid5-hex',
            'required': True
        },
        'userAgent': {
            'type': 'any',
            'description': ('User Agent from HTTP request. Either a '
                            'parsed object, parsed JSON string, or '
                            'original User-Agent string.'),
            'required': False
        }
    },
    'additionalProperties': False
}


def get_event_capsule_schema():
    return copy.deepcopy(EVENT_CAPSULE_SCHEMA)
