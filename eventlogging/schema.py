# -*- coding: utf-8 -*-
"""
  eventlogging.schema
  ~~~~~~~~~~~~~~~~~~~

  This module implements schema retrieval and validation. Schemas are
  referenced via SCIDs, which are tuples of (Schema name, Revision ID).
  Schemas are retrieved via HTTP and then cached in-memory. Validation
  uses :module:`jsonschema`.

"""
from __future__ import unicode_literals

import logging
import re

import jsonschema

import os
import socket
import time
import yaml

from jsonschema import ValidationError, SchemaError

from .compat import integer_types, json, http_get, string_types
from .utils import datetime_from_timestamp


import uuid

__all__ = (
    'CAPSULE_SCID', 'create_event_error', 'get_schema',
    'validate', 'scid_from_event',
    'init_schema_cache', 'get_from_meta',
    'id_from_event', 'schema_name_from_event', 'meta_from_event',
    'datetime_from_event'
)

# Regular expression which matches valid schema names.
SCHEMA_RE_PATTERN = r'[a-zA-Z0-9_-]{1,63}'
SCHEMA_RE = re.compile(r'^{0}$'.format(SCHEMA_RE_PATTERN))

# These REs will be used when constructing an ErrorEvent
# to extract the schema and revision out of a raw event
# string in the case it cannot be parsed as JSON.
RAW_SCHEMA_RE = re.compile(
    r'%22schema%22%3A%22({0})%22'.format(SCHEMA_RE_PATTERN)
)
RAW_REVISION_RE = re.compile(r'%22revision%22%3A(\d+)')

# URL of index.php on the schema wiki (same as
# '$wgEventLoggingSchemaApiUri').
SCHEMA_WIKI_API = 'https://meta.wikimedia.org/w/api.php'

# Template for schema article URLs. Interpolates SCIDs.
SCHEMA_URL_FORMAT = (
    SCHEMA_WIKI_API + '?action=jsonschema&title=%s&revid=%s&formatversion=2'
)


# Use this to extract schemas from a local file name
# File names look either like:
# SchemaName.Revison.json (e.g Edit.123.json)
# or
# SchemaName.json (e.g. Delete.json)
# If no revision is found in the file name,
# the schema will be saved as revision 0 in the schema_cache.
# NOTE:  Do not ever have an unrevisioned schema AND revisioned
# schemas of the same name.
SCHEMA_FILE_PATTERN = re.compile(r'([\w\-]+)\.(\d+)?(?:.)?(json|yaml|yml)')

# SCID of the metadata object which wraps each capsule-style event.
CAPSULE_SCID = ('EventCapsule', 10981547)

# TODO: Make new meta style EventError on meta.
ERROR_SCID = ('EventError', 14035058)

# Schemas retrieved via HTTP or files are cached in this dictionary.
schema_cache = {}

# A validator for each schema is created the first time
# the schema is used to validate an event.  Cache
# the validator for that schema here.
schema_validator_cache = {}


def init_schema_cache(schemas_path=None):
    """
    Clears any cached schemas and schema validators.
    If schemas_path is provided, load_local_schemas is called.
    """
    schema_cache.clear()
    schema_validator_cache.clear()
    if schemas_path:
        load_local_schemas(schemas_path=schemas_path)


def get_schema(scid, encapsulate=False):
    """
    Get schema from memory or HTTP.
    Latest revisions are saved in the schema_cache as revision 0.
    If you know a schema has already been cached, you can get the latest
    version of it by calling get_schema(('name', 0)).
    """
    validate_scid(scid)

    schema = None
    name, revision = scid

    # if there are cached schema revisions already under this schema name
    if name in schema_cache:
        # And if this revision is cached, use it
        if revision in schema_cache[name]:
            schema = schema_cache[name][revision]
        # Else if revision is 0 and the latest schema hasn't been cached yet
        # Look up the latest cached schema and cache it as 0.
        elif revision == 0:
            schema = get_latest_cached_schema(name)
            cache_schema(scid, schema)

    # Attempt to look up schema in http schema registry.
    if not schema:
        # For now, only pre cached schemas
        # can be accesed by revision 0 (latest).
        if revision <= 0:
            raise SchemaError(
                'Cannot lookup schema (%s,%s) '
                'from remote schema registry.' % scid
            )
        schema = http_get_schema(scid)
        # Save the retrieved schema for later
        cache_schema(scid, schema)

    # If we get this far and still don't have a schema, raise error.
    if not schema:
        raise SchemaError("Failed getting schema for scid %s,%s." % scid)

    # We depart from the JSON Schema specifications by disallowing
    # additional properties by default.
    # See `<https://bugzilla.wikimedia.org/show_bug.cgi?id=44454>`_.
    schema.setdefault('additionalProperties', False)
    if encapsulate:
        capsule = get_schema(CAPSULE_SCID)
        capsule['properties']['event'] = schema
        return capsule
    return schema


def cache_schema(scid, schema):
    """
    Saves the schema in the global schema_cache.
    The latest schema revision is saved as revision 0.
    """
    name, revision = scid

    if name not in schema_cache:
        schema_cache[name] = {}

    schema_cache[name][revision] = schema

    # Reset the latest revision (indicated by 0)
    # to the latest cached revision for faster lookup next time.
    schema_cache[name][0] = get_latest_cached_schema(name)
    return schema


def get_latest_cached_schema(name):
    """
    If name has schemas already cached, return the latest one.
    """
    if name not in schema_cache:
        return None

    return schema_cache[name][
        max(schema_cache[name].keys())
    ]


def validate_scid(scid):
    """Validates an SCID.
    :raises :exc:`ValidationError`: If SCID is invalid.
    """
    if not isinstance(scid, tuple):
        raise jsonschema.ValidationError('Invalid scid: %s' % scid)

    schema, revision = scid
    if not isinstance(revision, integer_types):
        raise ValidationError('Invalid revision ID type: %s' % type(revision))
    if revision < 0:
        raise ValidationError('Invalid revision ID %s' % revision)
    if not isinstance(schema, string_types) or not SCHEMA_RE.match(schema):
        raise ValidationError('Invalid schema name: %s' % schema)


def validate(event, encapsulate=True):
    """Validates an event.
    :raises :exc:`ValidationError`: If event is invalid.
    """
    # get scid from event object
    scid = scid_from_event(event)

    if scid[0] == 'unknown' or scid[1] == -1:
        # If `schema` or `revision` keys are missing, a KeyError
        # exception will be raised. We re-raise it as a
        # :exc:`ValidationError` to provide a simpler API for callers.
        raise ValidationError('scid could not be found in event')

    schema = get_schema(scid, encapsulate=encapsulate)

    # Get validator for this schema out of the cache, or
    # create a new validator and save it in the cache.
    if scid in schema_validator_cache:
        validator = schema_validator_cache[scid]
    else:
        validator = get_validator(schema)(schema)
        schema_validator_cache[scid] = validator

    validator.validate(event)


def get_validator(schema):
    """
    Returns a jsonschema validator for schema.
    If the $schema key is not set, this will default
    to Draft3Validator.
    :raises :exc `SchemaError`: If schema is invalid.
    """
    if not isinstance(schema, dict):
        raise SchemaError(
            'schema is not a dict instead is a %s' % type(schema)
        )

    return jsonschema.validators.validator_for(
        schema, default=jsonschema.Draft3Validator
    )


def http_get_schema(scid):
    """Retrieve schema via HTTP."""
    validate_scid(scid)
    url = SCHEMA_URL_FORMAT % scid
    try:
        schema = json.loads(http_get(url))

    except (ValueError, EnvironmentError) as ex:
        raise SchemaError('Schema fetch failure: %s' % ex)
    # Make sure the schema itself validates.
    get_validator(schema).check_schema(schema)
    return schema


def file_get_schema(file):
    """Retrieve schemas from SCHEMA_REPO_PATH."""
    if not os.path.isfile(file):
        return None

    try:
        with open(file) as f:
            schema = yaml.load(f)
    except (ValueError, EnvironmentError) as ex:
        raise SchemaError(
            'Schema fetch from file %s failure: %s' % (file, ex)
        )
    # Make sure the schema itself validates.
    get_validator(schema).check_schema(schema)

    return schema


def load_local_schemas(schemas_path):
    """
    Walks schemas_path looking for files that mach
    SCHEMA_FILE_PATTERN and loads them into the
    schema_cache by calling get_schema with the
    scid extractd from the matched filename.
    """
    # Loads all schemas found in path into
    # the in memory schema cache.
    logging.info("Loading local schemas from %s " % schemas_path)

    if not os.path.isdir(schemas_path):
        raise RuntimeError(
            "Could not load local schemas. "
            "%s is not a directory " % schemas_path
        )

    for path, subdirs, files in os.walk(schemas_path):
        for f in files:
            file = os.path.join(path, f)
            logging.info("Loading schema from %s" % f)
            scid = scid_from_filename(file)
            validate_scid(scid)

            if scid:
                cache_schema(
                    scid,
                    file_get_schema(file)
                )


def scid_from_filename(filename):
    """
    Extracts scid from filename based on the
    SCHEMA_FILE_PATTERN regex.  If filename doesn't
    match, this returns None.
    """
    match = SCHEMA_FILE_PATTERN.search(filename)
    if match:
        schema_name = match.group(1)
        if match.group(2):
            revision = int(match.group(2))
        else:
            # If revision cannot be extracted from the filename,
            # then default to revision 0.  There had better not
            # already be a schema of this name in the schema cache!!!
            revision = 0
        return schema_name, revision
    else:
        logging.warn("Could not extract scid from file %s" % filename)
        return None


def meta_from_event(event):
    """
    Returns the meta object for this event.
    """
    if 'meta' in event:
        meta = event['meta']
    # else use keys from EventCapsule as meta
    else:
        capsule_keys = get_schema(CAPSULE_SCID)['properties'].keys()
        # don't include 'event' in capsule keys, it is the event data.
        meta = {
            key: event[key] for key in capsule_keys if (
                key != 'event' and key in event
            )
        }
    return meta


def get_from_meta(key, event, default=None):
    """
    Returns the value for key in the meta object of this event.
    """
    return meta_from_event(event).get(key, default)


def id_from_event(event, default=None):
    """
    Returns the uuid for this event.
    """
    # Look for 'id' or 'uuid' in event meta data.
    return get_from_meta(
        'id',
        event,
        default=get_from_meta(
            'uuid',
            event,
            default=default
        ))


def datetime_from_event(event, default=None):
    """
    Looks for a timestamp in event meta data,
    parses this into a datetime.datetime object
    and returns it.
    """
    t = get_from_meta(
        'dt',
        event,
        default=get_from_meta(
            'timestamp',
            event,
            default=default
        )
    )
    # Just return if we couldn't find a timestamp in meta data.
    if t == default:
        return t
    else:
        return datetime_from_timestamp(t)


def schema_name_from_event(event):
    """
    Returns the schema name for this event.
    """
    return get_from_meta('schema', event, 'unknown')


def schema_revision_from_event(event, use_latest_revision=False):
    """
    Returns the schema revision for this event
    """
    if use_latest_revision:
        default = 0
    else:
        default = -1

    return get_from_meta('revision', event, default)


def scid_from_event(event, use_latest_revision=False):
    """
    Gets an scid out of the provided event object.
    If scid can not be extracted, returns ('unknown', -1)
    """
    return (
        schema_name_from_event(event),
        schema_revision_from_event(event, use_latest_revision)
    )


def schema_from_event(event, use_latest_revision=False, encapsulate=False):
    """
    Returns the json schema for this event.
    """
    return get_schema(
        scid_from_event(event, use_latest_revision),
        encapsulate=encapsulate
    )


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
    errored_schema = 'unknown'
    errored_revision = -1

    # If we've got a parsed event, then we can just get the schema
    # and revision out of the object.
    if parsed_event:
        errored_schema, errored_revision = scid_from_event(parsed_event)

    # otherwise attempt to get them out of the raw_event with a regex
    else:
        schema_match = RAW_SCHEMA_RE.search(raw_event)
        if schema_match:
            errored_schema = schema_match.group(1)

        revision_match = RAW_REVISION_RE.search(raw_event)
        if revision_match:
            errored_revision = int(revision_match.group(1))

    return {
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
    }
