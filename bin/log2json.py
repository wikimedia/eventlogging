#!/usr/bin/env python
# -*- coding: utf8 -*-
from __future__ import unicode_literals

import logging
import sys

import jsonschema
import zmq

from eventlogging.schema import get_schema
from eventlogging.compat import json, items, parse_qsl
from eventlogging.stream import zmq_subscribe
from eventlogging.utils import ncsa_to_epoch, hash_value


ENDPOINT = b'tcp://*:8484'
META_SCHEMA_REV = 4891798

logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)

# Maps compressed query param name of auto fields to human-readable
# name. I hate this because it veers off the path of explicitly modelled
# data and back toward the land of ad-hoc customizations. But the
# expansion of the max URL length to 1024 bytes will allow us to send
# raw URL-encoded JSON instead of key=val params, which will resolve
# some of this.
meta_readable = {
    '_rv': '_revision',
    '_id': '_schema',
    '_ok': '_valid',
    '_db': '_site'
}


# Maps JSON schema types to a function that will convert a string to
# that type. Ugly. Also going away when we move to URL-encoded JSON.
casters = {
    'integer': int,
    'array': lambda x: x.split(','),
    'boolean': lambda x: x.lower() == 'true',
    'null': lambda x: None,
    'number': lambda x: float(x) if '.' in x else int(x),
    'string': lambda x: x
}


def typecast(obj, schema):
    """
    Optimistically attempt to cast the value of each property to the
    type specified for that property by the schema.
    """
    properties = schema.get('properties', {})
    types = {k: v.get('type') for k, v in items(properties)}
    return {k: casters[types.get(k, 'string')](v) for k, v in items(obj)}


def decode_event(q):
    """
    Decodes a query string generated by EventLogging to a Python object
    by matching it with a schema and validating it accordingly.
    """
    q = dict(parse_qsl(q.strip('?;')))
    meta = {}
    e = {}
    for k, v in items(q):
        if k.startswith('_'):
            meta[k] = v
        else:
            e[k] = v

    metaschema = get_schema(META_SCHEMA_REV)
    meta = typecast(object, metaschema)

    schema = get_schema(meta['_rv'])
    e = typecast(e, schema)
    jsonschema.validate(e, schema)

    e['meta'] = {meta_readable.get(k, k): v for k, v in items(meta)}
    return e


def parse_bits_line(line):
    """Parse a log line emitted by varnishncsa on the bits hosts."""
    try:
        q, origin, seq_id, timestamp, client_ip = line.split()
        e = decode_event(q)
    except (ValueError, KeyError, jsonschema.ValidationError):
        logging.exception('Unable to decode: %s', line)
        return None

    e['meta'].update({
        'truncated': not q.endswith(';'),
        'origin': origin.split('.', 1)[0],
        'seqId': int(seq_id),
        'timestamp': ncsa_to_epoch(timestamp),
        'clientIp': hash_value(client_ip)
    })

    return e


if __name__ == '__main__':
    context = zmq.Context.instance()
    pub = context.socket(zmq.PUB)
    pub.bind(ENDPOINT)

    logging.info('Publishing JSON events on %s..', ENDPOINT.decode('utf8'))

    for raw_event in zmq_subscribe('tcp://localhost:8422'):
        event = parse_bits_line(raw_event)
        if event is not None:
			# We can't use pyzmq's Socket.send_json because it doesn't
			# send a trailing newline and our stream is line-oriented.
            pub.send_unicode(json.dumps(event) + '\n')
