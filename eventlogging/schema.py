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
import jsonschema
import os
import re
import yaml

from jsonschema import ValidationError, SchemaError

from .compat import integer_types, string_types, url_get, urisplit

__all__ = (
    'cache_schema', 'get_schema', 'validate', 'init_schema_cache',
    'is_schema_cached', 'get_latest_schema_revision', 'CAPSULE_SCID',
    'ERROR_SCID', 'SCHEMA_RE_PATTERN'
)

# Regular expression which matches valid schema names.
SCHEMA_RE_PATTERN = r'([\./a-zA-Z0-9_-]){1,63}'
SCHEMA_RE = re.compile(r'^{0}$'.format(SCHEMA_RE_PATTERN))

# URL of index.php on the schema wiki (same as
# '$wgEventLoggingSchemaApiUri').
# TODO: make this configurable
SCHEMA_WIKI_API = 'https://meta.wikimedia.org/w/api.php'

# Template for schema article URLs. Interpolates SCIDs.
SCHEMA_URL_FORMAT = (
    SCHEMA_WIKI_API + '?action=jsonschema&title=%s&revid=%s&formatversion=2'
)

# Use this to extract schemas from a local file name
# File names must look like:
# my/schema/name/<revision>.yaml (e.g mediawiki/page/create/123.yaml)
SCHEMA_URI_PATTERN = re.compile(r'([\w\-\./]+)/(\d+)(?:\.(?:json|yaml|yml))?$')

# SCID of the metadata object which wraps each capsule-style event.
CAPSULE_SCID = ('EventCapsule', 15423246)

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
    Get schema from memory or a URL.
    """
    validate_scid(scid)

    schema = None
    name, revision = scid

    # If there are cached schema revisions already under this schema name.
    if name in schema_cache and revision in schema_cache[name]:
        schema = schema_cache[name][revision]

    # Attempt to retrieve up schema its URL.
    if not schema:
        schema = retrieve_schema(scid)
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
    """
    name, revision = scid

    if name not in schema_cache:
        schema_cache[name] = {}

    schema_cache[name][revision] = schema

    return schema


def is_schema_cached(scid):
    """
    Returns True of if the scid is already in schema_cache, False otherwise.
    """
    return scid[0] in schema_cache and scid[1] in schema_cache[scid[0]]


# TODO: cache known latest schema instead of looking it up every time.
def get_latest_schema_revision(schema_name):
    """
    Given a schema_name, this returns the latest
    known revision for that schema.
    """
    if schema_name in schema_cache:
        return max(schema_cache[schema_name].keys())
    else:
        return None


def validate_scid(scid):
    """Validates an SCID.
    :raises :exc:`ValidationError`: If SCID is invalid.
    """
    if not isinstance(scid, tuple):
        raise jsonschema.ValidationError('Invalid scid: %s' % scid)

    schema, revision = scid
    if not isinstance(revision, integer_types):
        raise ValidationError('Invalid revision ID type: %s' % type(revision))
    if revision < 1:
        raise ValidationError('Invalid revision ID %s' % revision)
    if not isinstance(schema, string_types) or not SCHEMA_RE.match(schema):
        raise ValidationError('Invalid schema name: %s' % schema)


def validate(event, encapsulate=True):
    """Validates an event.
    :raises :exc:`ValidationError`: If event is invalid.
    """

    scid = event.scid()
    if not scid:
        # If scid is not found in this event, then we can't validate. Raise
        # :exc:`ValidationError` to provide a simpler API for callers.
        raise ValidationError('scid could not be extracted from event.')

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


def retrieve_schema(scid):
    """
    Gets the URL for this scid and then calls
    url_get_schema to get it.
    """
    return url_get_schema(url_from_scid(scid))


def url_get_schema(url):
    """Retrieve schema from URL."""
    try:
        schema = yaml.load(url_get(url))

    except (ValueError, EnvironmentError) as ex:
        raise SchemaError('Schema fetch from %s failed: %s' % (url, ex))
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
            # Skip this file if it isn't parseable yaml or json.
            if not (f.endswith('.yaml') or
                    f.endswith('.yml') or
                    f.endswith('.json')):
                continue

            full_path = os.path.join(path, f)
            url = 'file://' + full_path
            logging.info("Loading schema from %s" % url)
            scid = scid_from_uri(url, schemas_path)

            if scid:
                try:
                    validate_scid(scid)
                    schema = url_get_schema(url)
                except Exception as e:
                    logging.error(
                        "Failed loading schema from %s: %s" % (url, e)
                    )
                else:
                    # If we loaded a valid schema, cache it.
                    cache_schema(scid, schema)


# TODO:
# The SCHEMA_URL_FORMAT should be configurable to work with
# any remote repository.  For now, this function only works
# with the EventLogging schema repository in meta.wikimedia.org.
def url_from_scid(scid):
    """
    Constructs a URL from which a schema for this scid
    can be found.  This assumes that the schema you are looking
    can be found using SCHEMA_URL_FORMAT.
    """
    return SCHEMA_URL_FORMAT % scid


def scid_from_uri(schema_uri, base_path=None):
    """
    Extracts scid from uri path based on the
    SCHEMA_URI_PATTERN regex.  If uri doesn't
    match, this returns None.

    If base_path is given, it will be removed from the path before
    attempting to match against SCHEMA_URI_PATTERN.

    Usage:
        scid_from_uri('my/schema/1.yaml')
            -> ('my/schema/, 1)
        scid_from_uri('file:///base/my/schema/1.yaml', '/base')
            -> ('my/schema', 1)
    """

    uri_path = urisplit(schema_uri).path
    # If base_path was given, then remove it to get an unqualified
    # schema_uri, containing just the schema name and revision.
    if base_path:
        uri_path = uri_path.replace(base_path, '')

    match = SCHEMA_URI_PATTERN.search(uri_path)
    if match:
        return (match.group(1).strip(os.path.sep), int(match.group(2)))
    else:
        logging.error("Could not extract scid from %s" % schema_uri)
        return None
