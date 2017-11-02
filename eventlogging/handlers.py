# -*- coding: utf-8 -*-
"""
  eventlogging.handlers
  ~~~~~~~~~~~~~~~~~~~~~

  This class contains the set of event readers and event writers that ship with
  EventLogging. Event readers are generators that yield successive events from
  a stream. Event writers are coroutines that receive events and handle them
  somehow. Both readers and writers are designed to be configurable using URIs.
  :func:`eventlogging.drive` pumps data through a reader-writer pair.

"""
import collections
import glob
import imp
import inspect
import jsonschema
import logging
import logging.handlers
import os
import re
import socket
import sys
import statsd
import time
import traceback

from .compat import items, json
from .event import Event
from .factory import writes, reads, get_reader, get_writer
from .streams import stream, pub_socket, sub_socket, udp_socket
from .jrm import store_sql_events
from .topic import TopicNotFound
from .utils import uri_delete_query_item, kafka_ids

__all__ = ('load_plugins',)

# EventLogging will attempt to load the configuration file specified in the
# 'EVENTLOGGING_PLUGIN_DIR' environment variable if it is defined. If it is
# not defined, EventLogging will default to the value specified below.
DEFAULT_PLUGIN_DIR = '/usr/local/lib/eventlogging'
plugin_functions = {}


def load_plugins(path=None):
    """Load EventLogging plug-ins from `path`. Plug-in module names are mangled
    to prevent clobbering modules in the Python module search path."""
    if path is None:
        path = os.environ.get('EVENTLOGGING_PLUGIN_DIR', DEFAULT_PLUGIN_DIR)
    for plugin_path in glob.glob(os.path.join(path, '*.py')):
        # The name of the plugin module is the name of its file without .py
        plugin_name = plugin_path.split('/')[-1].replace('.py', '')
        module = imp.load_source('__eventlogging_plugin_%s__' % plugin_name,
                                 plugin_path)
        functions = [t[0] for t in inspect.getmembers(module,
                                                      inspect.isfunction)]
        for f in functions:
            if (f not in plugin_functions):
                plugin_functions[f] = module.__dict__[f]
            else:
                raise ValueError('Function %s already \
                                 defined by another plugin' % f)


def find_function(function_name):
    """
    Looks for the specified function, coming from a consumer's url parameter,
    in the functions object loaded on initialization of this module.

    Arguments:
        *function_name (str): name of the function specified in url params
    """
    if function_name in globals():
        return globals()[function_name]
    function = plugin_functions.get(function_name)
    if not function:
        raise NotImplementedError(
            'Function %s not implemented' % function_name)
    return function


#
# Writers
#

@writes('mongodb')
def mongodb_writer(uri, database='events'):
    import pymongo

    client = pymongo.MongoClient(uri)
    db = client[database]

    while 1:
        event = (yield)
        event['timestamp'] = event.datetime()
        event['_id'] = event.id()
        collection = event.schema_name()
        db[collection].insert(event)


# Can be addressed as default kafka:// handler, and as specific
# kafka client name, kafka-python://
@writes('kafka')
@writes('kafka-python')
def kafka_python_writer(
    path,
    topic=None,
    key=None,
    async=True,
    sync_timeout=2.0,
    blacklist=None,
    raw=False,
    identity=None,
    **kafka_args
):
    """
    Write events to Kafka.

    Kafka URIs look like:
    kafka:///b1:9092,b2:9092?topic=eventlogging_%s(schema)&async=True&...

    This producer KafkaProducer from the kafka-python library.
    You may pass any configs that base Producer takes
    as keyword arguments via URI query params.

    If the KafkaProducer kwarg 'retries' is not given
    in kafka_args (via URI query params), it will be set to default 3.
    This is to ensure that messages are retried upon routine metadata changes
    like partition leadership change.

    Arguments:
        *path (str): URI path should be comma separated Kafka Brokers.
            e.g. kafka01:9092,kafka02:9092,kafka03:9092

        *topic (str): Python format string topic name.
            If the incoming event is a dict (not a raw string)
            topic will be formatted against event.  I.e.
            topic.format(**event).  Default: None.

            If topic is None, the topic will be extracted from
            the event meta data rather than formatting
            against event.  This means that the 'topic' key must
            be in the event metadata. You _must_ provide a static
            topic if raw=True.

        *key (str): Format string key of the message in Kafka.
            The key will be formatted against event.  I.e.
            key.format(**event).  Default: None.
            If raw=True, formatting will not happen, and the
            key will be used exactly as set.

        *async (bool): If True, this will not block to wait for Kafka
            message ACKs before producing the next message.  Defaults to True.

        *sync_timeout (float): If async is False, then we will wait this
            number of seconds for the produce response to return.
            This paramater is ignored if async is True.

        *blacklist (str): Pattern string matching a list of schemas that
            should not be written. This is useful to keep high volume schemas
            from being written to an output stream.  This will
            be ignored if the incoming events are raw.

        *raw (bool): Should the incoming stream be treated as raw strings or
            as Events?  No topic or key interpolation will be done
            if raw is True.  Instead, topic and key will be used as provided.
            Defaults to False.

        *identity (str): Used as the prefix for the Kafka client id. If not
            given unique one will be generated.

    Yields:
        kafka.future.Future.  This is experimental.

    """
    from kafka import KafkaProducer

    # Cannot use raw without setting a specific topic to produce to.
    if raw and not topic:
        raise ValueError(
            'Cannot produce raw events to Kafka '
            'without setting topic parameter.'
        )

    # remove non KafkaProducer args from kafka_args
    kafka_args = {
        k: v for k, v in items(kafka_args)
        if k in KafkaProducer.DEFAULT_CONFIG
    }
    # If we are not using async, set default batch_size to 0.  This
    # will cause KafkaProducer to not do any batching.
    if not async and 'batch_size' not in kafka_args:
        kafka_args['batch_size'] = 0

    # kafka-python expects api_version to be a tuple of ints.
    # Convert the semvar string.
    if 'api_version' in kafka_args:
        kafka_args['api_version'] = tuple(
            [int(i) for i in str(kafka_args['api_version']).split('.')]
        )

    # Set default retries to 3.
    if 'retries' not in kafka_args:
        kafka_args['retries'] = 3

    # Get a kafka client_id based on identity
    (client_id, _) = kafka_ids(identity)

    # Resuable function for serializing string to utf-8 bytes.
    def serialize_string(s):
        return s.encode('utf-8')

    kafka_producer = KafkaProducer(
        # Brokers should be in the uri path
        bootstrap_servers=path.strip('/'),
        client_id=client_id,
        # Serialize keys as strings if we will be keying messages.
        key_serializer=serialize_string if key else None,
        # Serialize values as strings if raw, else assume they are Event dicts
        value_serializer=serialize_string if raw else Event.serialize,
        **kafka_args
    )

    if blacklist:
        blacklist_pattern = re.compile(blacklist)
    else:
        blacklist_pattern = None

    # Yielding response_future back from the coroutine send() call is
    # experimental.
    response_future = None
    while 1:
        event = (yield response_future)

        # If event is not raw and blacklist_pattern is set,
        # then check to see if we should skip this event.
        if not raw and blacklist_pattern:
            schema_name, revision = event.scid()
            if blacklist_pattern.match(schema_name):
                logging.debug(
                    '%s is blacklisted, not writing event %s.' %
                    (schema_name, event)
                )
                continue

        # Get the actual Kafka topic to which we will produce
        try:
            message_topic = topic.encode('utf-8') if raw else \
                event.topic(topic_format=topic).encode('utf-8')
        # If we failed getting topic, log and skip the event.
        except TopicNotFound as e:
            logging.error('%s.  Skipping event' % e)
            continue

        # Unless key is found, just use None.
        message_key = None
        # If not raw and key is set, then look for the key in the event.
        if not raw and key:
            try:
                message_key = key.format(**event)
            # If we failed getting key, log and skip the event.
            except KeyError as e:
                logging.error(
                    'Could not get message key from event. KeyError: %s. '
                    'Skipping event.' % e
                )
                continue

        # Produce the message.
        response_future = kafka_producer.send(
            message_topic, key=message_key, value=event
        )

        # If we didn't want async production, then get the
        # result of the future now.
        if not async:
            # This will raise an exception if the produce request
            # fails or is timed out.
            response_future.get(sync_timeout)
            response_future = None


# NOTE: kafka-confluent is experimental.
@writes('kafka-confluent')
def kafka_confluent_writer(
    path,
    topic=None,
    key=None,
    async=True,
    blacklist=None,
    raw=False,
    identity=None,
    **kwargs
):
    """
    Write events to Kafka.

    Kafka URIs look like:
    kafka:///b1:9092,b2:9092?topic=eventlogging_%s(schema)&async=True&...

    This uses the Producer from the librdkafka backed confluent-kafka
    python library. You may pass any configs that the librdkafka Producer
    take as keyword arguments via URI query params.

    Arguments:
        *path (str): URI path should be comma separated Kafka Brokers.
            e.g. kafka01:9092,kafka02:9092,kafka03:9092

        *topic (str): Python format string topic name.
            If the incoming event is a dict (not a raw string)
            topic will be formatted against event.  I.e.
            topic.format(**event).  Default: None.

            If topic is None, the topic will be extracted from
            the event meta data rather than formatting
            against event.  This means that the 'topic' key must
            be in the event metadata. You _must_ provide a static
            topic if raw=True.

        *key (str): Format string key of the message in Kafka.
            The key will be formatted against event.  I.e.
            key.format(**event).  Default: None.
            If raw=True, formatting will not happen, and the
            key will be used exactly as set.

        *async (bool): If True, this will not block to wait for Kafka
            message ACKs before producing the next message.  Defaults to True.
            If False and not otherwise specified, queue.buffering.max.ms and
            socket.blocking.max.ms will both be set to 1 to ensure
            higher throughput for synchronous production.  This may
            result in higher CPU usage for low volume clients.

        *blacklist (str): Pattern string matching a list of schemas that
            should not be written. This is useful to keep high volume schemas
            from being written to an output stream.  This will
            be ignored if the incoming events are raw.

        *raw (bool): Should the incoming stream be treated as raw strings or
            as Events?  No topic or key interpolation will be done
            if raw is True.  Instead, topic and key will be used as provided.
            Defaults to False.

        *identity (str): Used as the prefix for the Kafka client id. If not
            given unique one will be generated.

    """
    from confluent_kafka import Producer

    # Cannot use raw without setting a specific topic to produce to.
    if raw and not topic:
        raise ValueError(
            'Cannot produce raw events to Kafka '
            'without setting topic parameter.'
        )

    blacklist_pattern = re.compile(blacklist) if blacklist else None

    # Get a kafka client_id based on identity
    (client_id, _) = kafka_ids(identity)

    # Remove anything that we know is not going to be a valid Producer
    # parameter
    eventlogging_keys = ('port', 'hostname', 'uri')
    kafka_args = {k: kwargs[k] for k in kwargs if k not in eventlogging_keys}
    kafka_args['bootstrap.servers'] = path.strip('/')
    kafka_args['client.id'] = client_id

    # If specifying broker.version.fallback, it should be a string!
    if 'broker.version.fallback' in kafka_args:
        kafka_args['broker.version.fallback'] = str(
            kafka_args['broker.version.fallback']
        )

    # If we are not using async, set default queue.buffering.max.ms
    # and socket.blocking.max.ms to get fast sync produce.
    # This may cause extra CPU usage.
    if not async:
        if 'queue.buffering.max.ms' not in kafka_args:
            kafka_args['queue.buffering.max.ms'] = 1
        if 'socket.blocking.max.ms' not in kafka_args:
            kafka_args['socket.blocking.max.ms'] = 1

    kafka_producer = Producer(**kafka_args)

    while True:
        event = (yield)

        # If event is not raw and blacklist_pattern is set,
        # then check to see if we should skip this event.
        if not raw and blacklist_pattern:
            schema_name, revision = event.scid()
            if blacklist_pattern.match(schema_name):
                logging.debug(
                    '%s is blacklisted, not writing event %s.' %
                    (schema_name, event)
                )
                continue

        # Get the actual Kafka topic to which we will produce
        try:
            message_topic = topic.encode('utf-8') if raw else \
                event.topic(topic_format=topic).encode('utf-8')
        # If we failed getting topic, log and skip the event.
        except TopicNotFound as e:
            logging.error('%s.  Skipping event' % e)
            continue

        # Unless key is found, just use None.
        message_key = None
        # If not raw and key is set, then look for the key in the event.
        if not raw and key:
            try:
                message_key = key.format(**event)
            # If we failed getting key, log and skip the event.
            except KeyError as e:
                logging.error(
                    'Could not get message key from event. KeyError: %s. '
                    'Skipping event.' % e
                )
                continue

        message_value = event.encode('utf-8') if raw else \
            json.dumps(event, sort_keys=True).encode('utf-8')

        # Produce the message.
        kafka_producer.produce(message_topic, message_value, message_key)

        # If not async, the flush Kafka produce buffer now and block
        # until we are done.
        if not async:
            kafka_producer.flush()


@writes('mysql', 'sqlite')
def sql_writer(
    uri,
    replace=False,
    statsd_host='',
    batch_size=3000,
    batch_time=300
):
    """
    Writes to an RDBMS, creating tables for SCIDs and rows for events.
    Note that the default MySQL engine is TokuDB.  If your MySQL
    does not support TokuDB, then set the EVENTLOGGING_MYSQL_ENGINE
    environment variable to the engine you want to use.  E.g.

      export EVENTLOGGING_MYSQL_ENGINE=InnoDB

    :param uri:         SQLAlchemy bind URI.
    :param replace:     If true, INSERT REPLACE will be used.
    :param statsd_host: hostname of statsd instance to which insert stats will
                        be sent.
    :param batch_size:  Max number of events per schema to insert as a batch.
    :param batch_time:  Max seconds to wait before inserting a batch.
    """
    import sqlalchemy

    logger = logging.getLogger('Log')

    # Create a statsd client instance if statsd_host is specified
    stats = None
    if statsd_host:
        stats = statsd.StatsClient(statsd_host, 8125, prefix='eventlogging')

    # Don't pass non SQLAlchemy parameters to SQLAlchemy.
    for argname in inspect.getargspec(sql_writer)[0]:
        uri = uri_delete_query_item(uri, argname)

    meta = sqlalchemy.MetaData(bind=uri)
    if meta.bind.dialect.name == 'mysql':
        @sqlalchemy.event.listens_for(sqlalchemy.pool.Pool, 'checkout')
        def ping(dbapi_connection, connection_record, connection_proxy):
            # Just before executing an insert, call mysql_ping() to verify
            # that the connection is alive, and reconnect if necessary.
            dbapi_connection.ping(True)

    # For each SCID (schema, revision) we store an event batch and
    # the timestamp of the first event.
    events = collections.defaultdict(lambda: ([], time.time()))
    try:
        while True:
            event = (yield)

            # Group the event stream by schema (and revision)
            scid = event.scid()
            try:
                topic = event.topic()
            except TopicNotFound:
                topic = None
            batch_key = (scid, topic)
            batch_events, first_timestamp = events[batch_key]
            batch_events.append(event)
            # Whenever the batch reaches
            # the size specified by batch_size or it hasn't received events
            # for more than batch_time seconds it is flushed into mysql.
            if (len(batch_events) >= batch_size or
                    time.time() - first_timestamp >= batch_time):
                try:
                    store_sql_events(meta, scid, batch_events, replace=replace)
                except jsonschema.SchemaError as e:
                    logger.error(e.message)
                else:
                    if stats:
                        stats.incr('overall.inserted', len(batch_events))
                del events[batch_key]
    except Exception:
        t = traceback.format_exc()
        logger.warn('Exception caught %s', t)
        raise
    finally:
        # If there are any batched events remaining,
        # process them before exiting.
        for batch_key, (batch_events, _) in events.iteritems():
            scid = batch_key[0]
            try:
                store_sql_events(meta, scid, batch_events, replace=replace)
            except jsonschema.SchemaError as e:
                logger.error(e.message)
            else:
                if stats:
                    stats.incr('overall.inserted', len(batch_events))
        logger.info(
            'Finally finished inserting remaining events '
            'before exiting sql handler.'
        )


@writes('file')
def log_writer(path, raw=False):
    """Write events to a file on disk."""
    handler = logging.handlers.WatchedFileHandler(path)

    # We want to be able to support multiple file writers
    # within a given Python process, so uniquely
    # identify this logger within Python's logging
    # system by the file's path.
    log = logging.getLogger('Events-' + path)

    log.setLevel(logging.INFO)
    log.addHandler(handler)
    # Don't propagate these events to the global logger
    # used by eventlogging.  We don't want eventlogging
    # daemons to print these event logs to stdout or stderr
    # all the time.
    log.propagate = False

    while 1:
        event = (yield)
        if raw:
            log.info(event)
        else:
            log.info(json.dumps(event, sort_keys=True, check_circular=False))


@writes('tcp')
def zeromq_writer(uri, raw=False):
    """Publish events on a ZeroMQ publisher socket."""
    pub = pub_socket(uri)
    while 1:
        event = (yield)
        if raw:
            pub.send_unicode(event)
        else:
            pub.send_unicode(json.dumps(event,
                                        sort_keys=True,
                                        check_circular=False) + '\n')


@writes('statsd')
def statsd_writer(hostname, port, prefix='eventlogging.schema'):
    """Increments StatsD SCID counters for each event."""
    addr = socket.gethostbyname(hostname), port
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while 1:
        event = (yield)
        stat = prefix + '.%(schema)s:1|c' % event
        sock.sendto(stat.encode('utf-8'), addr)


@writes('stdout')
def stdout_writer(uri, raw=False):
    """Writes events to stdout. Pretty-prints if stdout is a terminal."""
    dumps_kwargs = dict(sort_keys=True, check_circular=False)
    if sys.stdout.isatty():
        dumps_kwargs.update(indent=2)
    while 1:
        event = (yield)
        if raw:
            print(event)
        else:
            print(json.dumps(event, **dumps_kwargs))


@writes('udp')
def udp_writer(hostname, port, raw=False):
    """Writes data to UDP."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while 1:
        event = (yield)
        if raw:
            sock.sendto(event, (hostname, port))
        else:
            sock.sendto(json.dumps(event), (hostname, port))


@writes('map')
def map_writer(uri, function):
    """
    Receives events and runs a map function on them.
    The map function is specified in the url's parameters.
    It should either return a new mapped event, or None if you want
    to exclude (filter) that event from the stream.

    Arguments:
        *uri: a writer uri, with a custom "map" scheme, e.g.
            map://{kafka_uri}?function={map_function}
        *function (str): name of the map function as given by the
            url's parameters
    """
    if not callable(function):
        function = find_function(function)

    # Remove 'map://'' and 'function' query arg from uri.
    writer_uri = uri_delete_query_item(uri.replace("map://", ""), 'function')

    writer = get_writer(writer_uri)

    while True:
        event = (yield)
        mapped_event = function(event)
        if mapped_event is not None:
            writer.send(mapped_event)


#
# Readers
#


@reads('stdin')
def stdin_reader(uri, raw=False):
    """Reads data from standard input."""
    return stream(sys.stdin, raw)


@reads('tcp')
def zeromq_subscriber(uri, identity=None, subscribe='', raw=False):
    """Reads data from a ZeroMQ publisher. If `raw` is truthy, reads
    unicode strings. Otherwise, reads JSON."""
    sock = sub_socket(uri, identity=identity, subscribe=subscribe)
    return stream(sock, raw)


@reads('udp')
def udp_reader(hostname, port, raw=False):
    """Reads data from a UDP socket."""
    return stream(udp_socket(hostname, port), raw)


# TODO: Remove this in favor of the more versitile
# map:// reader/writer after it is no longer used in
# production.  T179625
@reads('filter')
def filtered_reader(uri, function):
    """
    Receives events and runs a filtering function on them.
    The filtering function is specified in the url's parameters.

    Arguments:
        *uri: a reader uri, with a custom "filter" scheme, e.g.
            filter://{kafka_uri}?function={filter_function}
        *function (str): name of the filtering function as given by the
            url's parameters
    """
    filter_function = find_function(function)
    reader_uri = uri.replace("filter://", "")
    return (e for e in get_reader(reader_uri) if filter_function(e))


@reads('map')
def map_reader(uri, function):
    """
    Receives events and runs a map function on them.
    The map function is specified in the url's parameters.
    It should either return a new mapped event, or None if you want
    to exclude (filter) that event from the stream.

    Arguments:
        *uri: a reader uri, with a custom "map" scheme, e.g.
            map://{kafka_uri}?function={map_function}
        *function (str): name of the map function as given by the
            url's parameters
    """
    if not callable(function):
        function = find_function(function)

    # Remove 'map://'' and 'function' query arg from uri.
    reader_uri = uri_delete_query_item(uri.replace("map://", ""), 'function')

    # Apply map function to each item in the stream to create a new stream.
    new_stream = (function(e) for e in get_reader(reader_uri))
    # Return a new stream with Nones removed.
    return (e for e in new_stream if e is not None)


# Can be addressed as default kafka:// handler, and as specific
# kafka client name, kafka-python://
@reads('kafka')
@reads('kafka-python')
def kafka_python_reader(
    path,
    topics=None,
    topic=None,
    identity=None,
    raw=False,
    **kafka_args
):
    """
    Reads events from Kafka.

    Kafka URIs look like:
    kafka:///b1:9092,b2:9092?topics=topic1,topic2&identity=consumer_group_name&
    &auto_commit_interval_ms=1000...

    This reader uses the kafka-python KafkaConsumer.  You may pass
    any configs that KafkaConsumer takes as keyword arguments as
    URI query params.

    auto_commit_interval_ms is by default 5 seconds.

    If auto_commit_enable is True, then messages will be marked as done based
    on the auto_commit_interval_ms time period.
    This has the downside of committing message offsets before
    work might be actually complete.  E.g. if inserting into MySQL, and
    the process dies somewhere along the way, it is possible
    that message offsets will be committed to Kafka for messages
    that have not been inserted into MySQL.  Future work
    will have to fix this problem somehow.  Perhaps a callback?

    The 'topic' parameter is provided for backwards compatibility.
    It will be used if topics is not given.

    Arguments:
        *path (str): Comma separated list of broker hostname:ports.

        *topics (list): List of topics to subscribe to.

        *topic (string): (deprecated) topic to subscribe to.  Use topics.

        *identity (str): Used as the Kafka consumer group id, and the prefix of
            the Kafka client id.  If not given, a new unique identity will
            be created.

        *raw (bool): If True, the generator returned will yield a stream of
            strings, else a stream of Events.
    """
    # Support use of deprecated topic arg, for now.
    if topic and not topics:
        topics = topic
        logging.warn('kafka \'topic\' argument is deprecated, use \'topics\'')

    if not topics:
        raise ValueError(
            'Cannot consume from Kafka without providing topics.'
        )

    from kafka import KafkaConsumer

    # Get kafka client_id and group_id based on identity.
    (client_id, group_id) = kafka_ids(identity)

    # Use topics as an array.
    if type(topics) != list:
        topics = topics.split(',')

    # remove non KafkaConsumer args from kafka_args
    kafka_args = {
        k: v for k, v in items(kafka_args)
        if k in KafkaConsumer.DEFAULT_CONFIG
    }

    # kafka-python expects api_version to be a tuple of ints.
    # Convert the semvar string.
    if 'api_version' in kafka_args:
        kafka_args['api_version'] = tuple(
            [int(i) for i in str(kafka_args['api_version']).split('.')]
        )

    # Be flexible with auto_offset_reset values.  The enum names
    # have changed in different clients and versions, but the int
    # values have never changed.  Allow setting this by int value on
    # kafka-python handler.  kafka-python itself already
    # handles the deprecated enum names 'smallest' and 'largest'.
    if ('auto_offset_reset' in kafka_args and
            kafka_args['auto_offset_reset'] in (-1, -2)):
        logging.warn(
            'kafka auto_offset_reset int values are deprecated, use '
            'either \'earliest\' or \'latest\''
        )
        # Map integer values to names that kafka-python accepts.
        kafka_args['auto_offset_reset'] = {
            -1: 'latest',
            -2: 'earliest',
        }.get(kafka_args['auto_offset_reset'])

    kafka_consumer = KafkaConsumer(
        # Brokers should be in the URI path.
        bootstrap_servers=path.strip('/'),
        group_id=group_id,
        client_id=client_id,
        **kafka_args
    )

    logging.info(
        'Consuming topics %s from Kafka in group %s as %s',
        topics,
        kafka_consumer.config['group_id'],
        kafka_consumer.config['client_id']
    )
    # Subscribe to list of topics.
    kafka_consumer.subscribe(topics)

    # Return a stream of message values.
    return stream((message.value for message in kafka_consumer), raw)


# NOTE: kafka-confluent is experimental.
@reads('kafka-confluent')
def kafka_confluent_reader(
    path,
    topics=None,
    topic=None,  # deprecated
    identity=None,
    raw=False,
    poll_timeout=1.0,
    **kwargs
):
    """
    Reads events from Kafka.

    Kafka URIs look like:
    kafka:///b1:9092,b2:9092?topics=topic1,topic2&identity=consumer_group&
    &auto.commit.interval.ms=1000...

    This uses the Consumer from the librdkafka backed confluent-kafka
    python library.  You may pass any configs that the librdkafka Consumer
    take as keyword arguments via URI query params.

    auto.commit.interval.ms is by default 5 seconds.

    If enable.auto.commit is True (the default), then messages will be marked
    as done based on the auto.commit.interval.ms time period.
    This has the downside of committing message offsets before
    work might be actually complete.  E.g. if inserting into MySQL, and
    the process dies somewhere along the way, it is possible
    that message offsets will be committed to Kafka for messages
    that have not been inserted into MySQL.  Future work
    will have to fix this problem somehow.  Perhaps a callback?

    The 'topic' parameter is provided for backwards compatibility.
    It will be used if topics is not given.

    Arguments:
        *path (str): Comma separated list of broker hostname:ports.

        *topics (list): List of topics to subscribe to.

        *topic (str): Deprecated topic to subscribe to.  Use topics instead.
            Ignored if topics is provided.

        *identity (str): Used as the Kafka consumer group.id, and the prefix
            of the Kafka client.id.  If not given, a new unique identity will
            be created.

        *raw (bool): If True, the generator returned will yield a stream of
            strings, else a stream of Events.  Default: False.

        *poll_timeout (float) Timeout in seconds to use for call to
            consumer.poll().  poll will only block for this long
            if there are no messages.  Default: 1.0.
    """
    if not topics and not topic:
        raise ValueError(
            'Cannot consume from Kafka without providing topics.'
        )

    from confluent_kafka import Consumer, KafkaError
    import signal

    # Use topics as an array if given, else just use topic
    topics = topics.split(',') if topics else [topic]

    # Get kafka client_id and group_id based on identity.
    (client_id, group_id) = kafka_ids(identity)

    # Remove anything that we know is not going to be a valid
    # Kafka Consumer parameter from kwargs and then set some required
    # configs.
    eventlogging_keys = ('port', 'hostname', 'uri')
    kafka_args = {k: kwargs[k] for k in kwargs if k not in eventlogging_keys}
    kafka_args['bootstrap.servers'] = path.strip('/')
    kafka_args['group.id'] = group_id
    kafka_args['client.id'] = client_id

    kafka_consumer = Consumer(**kafka_args)

    logging.info(
        'Consuming topics %s from Kafka in group %s as %s',
        topics, group_id, client_id
    )

    # Callback for logging during consumer rebalances
    def log_assign(consumer, partitions):
        logging.info('Partition assignment change for %s. Now consuming '
                     'from %s partitions: %s',
                     client_id, len(partitions), partitions)

    # Subscribe to list of topics.
    kafka_consumer.subscribe(topics, on_assign=log_assign)

    # Define a generator to read from the Consumer instance.
    def consume(consumer, timeout=1.0):
        # Make sure we close the consumer on SIGTERM.
        # SIGINT should be caught by the finally in consume().
        def shutdown_handler(_signo, _stack_frame):
            logging.info('Caught SIGTERM, closing KafkaConsumer %s '
                         'to commit outstanding offsets.', client_id)
            consumer.close()
            sys.exit(0)
        signal.signal(signal.SIGTERM, shutdown_handler)

        # Wrap the poll loop in a try/finally.
        try:
            while True:
                # Poll for messages
                message = consumer.poll(timeout=timeout)

                # If no message was found in timeout, poll again.
                if not message:
                    continue

                # Else if we encountered a KafkaError, log and continue.
                elif message.error():
                    # _PARTITION_EOF is pretty normal, just log at debug
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        logging.debug(
                            'KafkaConsumer %s consuming %s [%d] '
                            'reached end at offset %d\n' % (
                                client_id,
                                message.topic(),
                                message.partition(),
                                message.offset()
                            )
                        )
                    # Else this is a real KafkaError, log at error.
                    else:
                        logging.error(message.error())

                # Else we got a proper message, yield it.
                else:
                    yield message.value()
        except BaseException as e:
            error_message = 'Exception while KafkaConsumer %s consuming' % (
                client_id
            )
            # Add more info if message is defined.
            if message:
                error_message += ' from %s [%s] at offset %s' % (
                    message.topic(), message.partition(), message.offset(),
                )
            logging.error(error_message)
            if (type(e) != KeyboardInterrupt):
                raise(e)
        finally:
            logging.info('Finally closing KafkaConsumer %s '
                         'to commit outstanding offsets.', client_id)
            consumer.close()

    # Return a stream of message values.
    return stream(consume(kafka_consumer, poll_timeout), raw)
