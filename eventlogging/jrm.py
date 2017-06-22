# -*- coding: utf-8 -*-
"""
  eventlogging.jrm
  ~~~~~~~~~~~~~~~~

  This module provides a simple object-relational mapper for JSON
  schemas and the objects they describe (hence 'jrm').

"""
from __future__ import division, unicode_literals

import collections
import datetime
import itertools
import logging
import _mysql
import os
import re
import sqlalchemy
import time

from .compat import items, json
from .schema import get_schema
from .topic import TopicNotFound
from .utils import flatten


__all__ = ('store_sql_events',)


# Format string for :func:`datetime.datetime.strptime` for MediaWiki
# timestamps. See `<https://www.mediawiki.org/wiki/Manual:Timestamp>`_.
MEDIAWIKI_TIMESTAMP = '%Y%m%d%H%M%S'


def event_to_table_name(event, with_schema_revision=True):
    """
    Returns the table name that should be used for this event.  This starts
    by attempting to use the event's topic if it has one, otherwise
    it will just use the event's schema name..  E.g.

    topic_name_1, schema_name_1235, etc.

    :param Event                event
    :param with_schema_revision If True, schema revision will be
                                appended to name.  Default: True
    """
    try:
        name = event.topic()
    except TopicNotFound:
        name = event.schema_name()

    table_name = re.sub('[^A-Za-z0-9]+', '_', name)
    if with_schema_revision:
        return '{}_{}'.format(table_name, event.schema_revision())
    else:
        return table_name



# An iterable of properties that should not be stored in the database.
NO_DB_PROPERTIES = (
    'recvFrom', 'revision', 'schema', 'seqId', 'topic'
)

# A dictionary mapping database engine names to table defaults.
ENGINE_TABLE_OPTIONS = {
    'mysql': {
        'mysql_charset': 'utf8',
        'mysql_engine': os.environ.setdefault(
            'EVENTLOGGING_MYSQL_ENGINE', 'TokuDB'
        )
    }
}

# Maximum length for string and string-like types. Because InnoDB limits index
# columns to 767 bytes, the maximum length for a utf8mb4 column (which
# reserves up to four bytes per character) is 191 (191 * 4 = 764).
STRING_MAX_LEN = 1024

# Default table column definition, to be overridden by mappers below.
COLUMN_DEFAULTS = {'type_': sqlalchemy.Unicode(STRING_MAX_LEN)}


class MediaWikiTimestamp(sqlalchemy.TypeDecorator):
    """A :class:`sqlalchemy.TypeDecorator` for MediaWiki timestamps."""

    # Timestamps are stored as VARCHAR(14) columns.
    impl = sqlalchemy.Unicode(14)

    def process_bind_param(self, value, dialect=None):
        """Convert an integer timestamp (specifying number of seconds or
        miliseconds since UNIX epoch) to MediaWiki timestamp format."""
        if value > 1e12:
            value /= 1000
        value = datetime.datetime.utcfromtimestamp(value).strftime(
            MEDIAWIKI_TIMESTAMP)
        if hasattr(value, 'decode'):
            value = value.decode('utf-8')
        return value

    def process_result_value(self, value, dialect=None):
        """Convert a MediaWiki timestamp to a :class:`datetime.datetime`
        object."""
        return datetime.datetime.strptime(value, MEDIAWIKI_TIMESTAMP)


class JsonSerde(sqlalchemy.TypeDecorator):
    """A :class:`sqlalchemy.TypeDecorator` for converting to and from JSON strings."""

    impl = sqlalchemy.Unicode(STRING_MAX_LEN)

    def process_bind_param(self, value, dialect=None):
        """Convert the value to a JSON string"""
        value = json.dumps(value)
        if hasattr(value, 'decode'):
            value = value.decode('utf-8')
        return value

    def process_result_value(self, value, dialect=None):
        """Convert a JSON string into a Python object"""
        return json.loads(value)


# Mapping of JSON Schema attributes to valid values. Each value maps to
# a dictionary of options. The options are compounded into a single
# dict, which is then used as kwargs for :class:`sqlalchemy.Column`.
#
# ..note::
#
#   The mapping is keyed in order of increasing specificity. Thus a
#   JSON property {"type": "number", "format": "utc-millisec"} will
#   map onto a :class:`MediaWikiTimestamp` type, and not
#   :class:`sqlalchemy.Float`.
mappers = collections.OrderedDict((
    ('type', {
        'boolean': {'type_': sqlalchemy.Boolean},
        'integer': {'type_': sqlalchemy.BigInteger},
        'number': {'type_': sqlalchemy.Float},
        'string': {'type_': sqlalchemy.Unicode(STRING_MAX_LEN)},
        # Encode arrays as JSON strings.
        'array': {'type_': JsonSerde},
    }),
    ('format', {
        'utc-millisec': {'type_': MediaWikiTimestamp, 'index': True},
        'uuid5-hex': {'type_': sqlalchemy.CHAR(32), 'index': True,
                      'unique': True},
    }),
    ('required', {
        # Note that required=true makes the column nullable anyway.
        # This is made so, to allow for partial purging of a schema,
        # to follow the data retention guidelines. See:
        # EventLogging/Data_retention_and_auto-purging in wikitech.
        # Still, the eventlogging_processor, will verify that the
        # required field is present, or discard the event otherwise.
        True: {'nullable': True},
        False: {'nullable': True}
    })
))


def typecast(property):
    """Generates a SQL column definition from a JSON Schema property
    specifier."""
    options = COLUMN_DEFAULTS.copy()
    for attribute, mapping in items(mappers):
        value = property.get(attribute)
        options.update(mapping.get(value, ()))
    return sqlalchemy.Column(**options)


def get_table(meta, scid, table_name, should_encapsulate=True):
    """Acquire a :class:`sqlalchemy.schema.Table` object for a JSON
    Schema specified by `scid`."""
    #  +---------------------------------+
    #  | Is description of table present |
    #  | in Python's MetaData object?    |
    #  +----+----------------------+-----+
    #       |                      |
    #       no                     yes
    #       |                      |      +---------------------+
    #       |                      +----->| Assume table exists |
    #       v                             | in DB               |
    #  +--------------------------+       +-----------+---------+
    #  | Describe table structure |                   |
    #  | using schema.            |                   |
    #  +------------+-------------+                   |
    #               |                                 |
    #               v                                 |
    #  +---------------------------+                  |
    #  | Does a table so described |                  |
    #  | exist in the database?    |                  |
    #  +----+-----------------+----+                  |
    #       |                 |                       |
    #       no                yes                     |
    #       |                 |                       |
    #       v                 |                       |
    #   +--------------+      |                       |
    #   | CREATE TABLE |      |                       |
    #   +---+----------+      |                       v
    #       |                 |         +-------------+------------+
    #       +-----------------+-------->| Return table description |
    #                                   +--------------------------+
    try:
        return meta.tables[table_name]
    except KeyError:
        return declare_table(meta, scid, table_name, should_encapsulate)


def declare_table(meta, scid, table_name, should_encapsulate=True):
    """Map a JSON schema to a SQL table. If the table does not exist in
    the database, issue ``CREATE TABLE`` statement."""
    schema = get_schema(scid, encapsulate=should_encapsulate)

    columns = schema_mapper(schema)

    table_options = ENGINE_TABLE_OPTIONS.get(meta.bind.name, {})

    table = sqlalchemy.Table(table_name, meta, *columns, **table_options)
    table.create(checkfirst=True)

    return table


def _insert_sequential(table, events, replace=False):
    """Insert events into the database by issuing an INSERT for each one."""
    for event in events:
        insert = table.insert(values=event)
        if replace:
            insert = (insert
                      .prefix_with('IGNORE', dialect='mysql')
                      .prefix_with('OR REPLACE', dialect='sqlite'))
        try:
            insert.execute()
        except sqlalchemy.exc.IntegrityError as e:
            # If we encouter a MySQL Duplicate key error,
            # just log and continue.
            if type(e.orig) == _mysql.IntegrityError and e.orig[0] == 1062:
                logging.error(e)
        except sqlalchemy.exc.ProgrammingError:
            table.create()
            insert.execute()


def _insert_multi(table, events, replace=False):
    """Insert events into the database using a single INSERT."""
    insert = table.insert(values=events)
    if replace:
        insert = (insert
                  .prefix_with('IGNORE', dialect='mysql')
                  .prefix_with('OR REPLACE', dialect='sqlite'))
    try:
        insert.execute()
    except sqlalchemy.exc.IntegrityError as e:
        # If we encouter a MySQL Duplicate key error,
        # just log and continue.  Note that this will
        # fail inserts for all events in this batch,
        # not just the one that caused a duplicate
        # key error.  In this case, should we just
        # call _insert_sequential() with these events
        # so that each event has a chance to be inserted
        # separately?
        if type(e.orig) == _mysql.IntegrityError and e.orig[0] == 1062:
            logging.error(e)
    except sqlalchemy.exc.SQLAlchemyError:
        table.create(checkfirst=True)
        insert.execute()


def insert_sort_key(event):
    """Sort/group function that batches events that have the same
    set of fields. Please note that schemas allow for optional fields
    that might/might not have been included in the event.
    """
    return tuple(sorted(event))


def store_sql_events(meta, scid, scid_events, replace=False):
    """Store a batch of events in the database.
    It assumes that all events belong to the same scid."""
    if len(scid_events) == 0:
        return
    logger = logging.getLogger('Log')

    dialect = meta.bind.dialect
    if (getattr(dialect, 'supports_multivalues_insert', False) or
            getattr(dialect, 'supports_multirow_insert', False)):
        insert = _insert_multi
    else:
        insert = _insert_sequential

    # Each event here should be the same schema, so we can
    # check if the first event should be encapsulated and also
    # use it to construct a table name to use that when constructing
    # the create table statement in declare_table.
    should_encapsulate = scid_events[0].should_encapsulate()
    table_name = event_to_table_name(scid_events[0])

    prepared_events = [prepare(e) for e in scid_events]
    # TODO: Avoid breaking the inserts down by same set of fields,
    # instead force a default NULL, 0 or '' value for optional fields.
    prepared_events.sort(key=insert_sort_key)
    for _, grouper in itertools.groupby(prepared_events, insert_sort_key):
        events = list(grouper)
        table = get_table(meta, scid, table_name, should_encapsulate)

        insert_started_at = time.time()
        insert(table, events, replace)
        insert_time_taken = time.time() - insert_started_at

        # The insert operation is all or nothing - either all events have
        # been inserted successfully (sqlalchemy wraps the insertion in a
        # transaction), or an exception is thrown and it's not caught
        # anywhere. This means that if the following line is reached,
        # len(events) events have been inserted, so we can log it.
        logger.info(
            'Inserted %d %s events in %f seconds',
            len(events), table_name, insert_time_taken
        )


def _property_getter(item):
    """Mapper function for :func:`flatten` that extracts properties
    and their types from schema."""
    key, val = item
    if isinstance(val, dict):
        if 'properties' in val:
            val = val['properties']
        elif 'type' in val:
            val = typecast(val)
    return key, val


def prepare(event):
    """Prepare an event for insertion into the database."""
    flat_event = flatten(event)
    for prop in NO_DB_PROPERTIES:
        flat_event.pop(prop, None)
    return flat_event


def column_sort_key(column):
    """Sort key for column names. 'id' and 'uuid' come first, then the
    top-level properties in alphabetical order, followed by the nested
    properties (identifiable by the presence of an underscore)."""
    return (
        ('id', 'uuid', column.name).index(column.name),
        column.name.count('_'),
        column.name,
    )


def schema_mapper(schema):
    """Takes a schema and map its properties to database column
    definitions."""
    properties = {k: v for k, v in items(schema.get('properties', {}))
                  if k not in NO_DB_PROPERTIES}

    columns = [sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True)]

    for name, col in items(flatten(properties, f=_property_getter)):
        col.name = name
        columns.append(col)

    columns.sort(key=column_sort_key)
    return columns
