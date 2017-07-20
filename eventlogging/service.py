# -*- coding: utf-8 -*-
"""
  eventlogging.service
  ~~~~~~~~~~~~~~~~~~~

  This module implements an HTTP service for producing events to
  EventLogging writers.

"""

import tornado.ioloop
import tornado.web
import tornado.gen
import tornado.escape
import tornado.httpserver

# For UnicodeError
from _codecs import *  # noqa
import logging
import os
import socket
import yaml

from . import ValidationError, SchemaError  # these are int __init__.py
from .compat import json
from .event import create_event_error, Event
from .factory import apply_safe, get_writer
from .schema import (
    cache_schema, init_schema_cache, is_schema_cached, validate,
    scid_from_uri, get_schema, get_cached_schema_uris,
    get_latest_schema_revision
)
from .topic import (
    get_topic_config, is_topic_configured, init_topic_config,
    latest_scid_for_topic, schema_allowed_in_topic, schema_name_for_topic,
    TopicNotConfigured, TopicNotFound, update_topic_config
)

# These must be checked before we import sprockets because
# sprockets also sets the envrionment variables if they are not set.
# Set the default sprockets.mixins.statsd prefix.
os.environ.setdefault('STATSD_PREFIX', 'eventlogging.service')
# Don't report per host stats by default.
os.environ.setdefault('STATSD_USE_HOSTNAME', 'False')

# sprockets.clients.statsd expects STATSD_HOST and STATSD_PORT to be set.
# If we don't give STATSD_HOST as an IP, the socket.sendto call in that
# library will end up doing a DNS lookup for every statsd communication.
# Lookup STATSD_HOST IP now.
# See also: https://phabricator.wikimedia.org/T171048
if 'STATSD_HOST' in os.environ:
    os.environ['STATSD_HOST'] = socket.gethostbyname(os.getenv('STATSD_HOST'))

from sprockets.mixins import statsd  # noqa


# Path to swagger spec file.
# This will be returned to HTTP requests
# to /?spec.
SWAGGER_SPEC_PATH = os.path.join(
    os.path.dirname(__file__), 'service-spec.yaml'
)
# Load the swagger spec.
with open(SWAGGER_SPEC_PATH) as f:
    swagger_spec = yaml.load(f)

# Use the topic that the swagger spec x-ample specifies
# for allowing automated monitoring POSTs.
spec_test_topic = swagger_spec['paths']['/v1/events']['post']['x-amples'][0]['request']['body']['meta']['topic']  # noqa
spec_test_scid = ('test_event', 1)


class SchemaNotAllowedInTopic(Exception):
    pass


class EventLoggingService(tornado.web.Application):
    """
    EventLogging HTTP Produce Service\n

    Routes:

      POST /v1/events
      GET  /v1/topics
      GET  /?spec

    NOTE: If you are writing events to Kafka, you should make sure that you
    configure your kafka writer with async=False.  This will allow you
    to be sure that a 201 response will not be returned to the HTTP client
    until your event is ACKed by Kafka.
    """

    def __init__(
        self,
        writer_uris,
        error_writer_uri=None,
        port=8085,
        num_processes=1
    ):
        """
        Note: you should call init_schemas_and_topic_config()
        before you instantiate an EventLoggingService.

        :param writer_uris: A list of EventLogging writer_uris.  Each valid
        event will be written to each of these writers.

        :param error_writer_uri: If configured, EventErrors will be written
        to this writer.

        :param port: Port on which to listen for HTTP requests.

        :num_processes: Number of processes to fork.
        """

        routes = [
            # POST /v1/events
            (r"/v1/events", EventHandler),

            # GET /v1/topics
            (r"/v1/topics", TopicConfigHandler),

            # GET /v1/schemas (or /v1/schemas/)
            # GET /v1/schemas/:schema_uri
            (r"/v1/schemas(?:/?|/(?P<schema_uri>.+))", SchemasHandler),

            # GET /?spec
            (r'[/]?', SpecHandler),
        ]

        super(EventLoggingService, self).__init__(routes)

        self.server = tornado.httpserver.HTTPServer(self)
        self.server.bind(port)
        # Torando will fork now.  We need to fork before
        # we instantiate eventlogging writers.
        self.server.start(int(num_processes))

        # Valid events will be sent to each of these writers.
        # Save the writer_uris as keys so that we can restart
        # failed writers.
        self.writers = {}
        for uri in writer_uris:
            self.writers[uri] = get_writer(uri)
            logging.info('Publishing valid JSON events to %s', uri)

        # Errored events will be written to this writer.
        if error_writer_uri:
            self.error_writer = get_writer(error_writer_uri)
            logging.info('Publishing errored events to %s.', error_writer_uri)
        else:
            self.error_writer = None

    def send(self, event):
        """Send the event to configured eventlogging writers."""
        for uri in self.writers.keys():
            w = self.writers[uri]
            try:
                w.send(event)
            # If the writer coroutine has stopped (likley due to
            # an error during the previous send()), attempt to
            # recreate the writer now.
            except StopIteration as e:
                logging.error(
                    "Writer %s has stopped: %s.  Attempting to restart." %
                    (uri, e)
                )
                w = get_writer(uri)
                self.writers[uri] = w
                w.send(event)

    def process_event(self, event):
        """
        Validate the event using the schema configured for it's topic.
        A valid event will be sent to the configured writers.

        Returns True on success, otherwise some Exception will be thrown.

        """
        topic = event.topic()
        scid = event.scid()

        # If no schema name was set on this event,
        # use the one configured for this topic.
        if not scid:
            scid = latest_scid_for_topic(topic)
            # Fill in scid / schema_uri for this even
            # from the topic config
            logging.debug(
                '%s did not set scid/schema_uri. Setting to %s,%s '
                'for topic %s' % (event, scid[0], scid[1], topic)
            )
            event.set_scid(scid)
        # Else at least the schema name was set.
        else:
            # If schema revision was not set, then we default to the latest.
            if not scid[1]:
                scid = (scid[0], get_latest_schema_revision(scid[0]))
                logging.debug(
                    '%s set schema name but not schema revision. Defaulting '
                    'schema to latest scid to %s,%s' %
                    (event, scid[0], scid[1])
                )
                event.set_scid(scid)

            # Make sure the provided event schema is allowed in this topic.
            if not schema_allowed_in_topic(scid[0], topic):
                raise SchemaNotAllowedInTopic(
                    'Events of schema %s are not allowed in topic %s. '
                    'Expected schema %s' % (
                        scid[0], topic,  schema_name_for_topic(topic)
                    )
                )

        validate(event, encapsulate=event.should_encapsulate())

        # Send this processed event to all configured writers
        # This will block until each writer finishes writing
        # this event.
        self.send(event)
        return True

    def handle_events(self, events, callback=None):
        """
        Calls process_event on each of the events.  Any
        errors thrown by process_event will be caught, and EventError
        objects will be returned describing the error that the offending
        event caused.

        :param events: list of event dicts
        """
        event_errors = []
        for event in events:
            error_message = None

            try:
                self.process_event(event)

            except TopicNotConfigured as e:
                error_message = str(e)

            except TopicNotFound as e:
                error_message = 'Could not get topic from %s. %s' % (
                    event, e
                )

            except SchemaNotAllowedInTopic as e:
                error_message = str(e)

            except SchemaError as e:
                error_message = 'Could not find schema for provided topic ' \
                    'in %s. %s' % (event, e)

            except ValidationError as e:
                error_message = 'Failed validating %s. %s ' % (
                    event,
                    e.message
                )

            # Catch generic Exceptions as well.  Note that if a configured
            # error output writer fails too, an exception will be
            # thrown and not caught during the error_writer.send() call.
            except Exception as e:
                error_message = 'Failed sending event %s. %s: %s' % (
                    event,
                    type(e).__name__,
                    e.message
                )

            finally:
                # If we encountered an error while processing this event,
                # log it and create an EventError that will be returned.
                if error_message:
                    logging.error("Failed processing event: %s", error_message)
                    event_error = create_event_error(
                        json.dumps(event),
                        error_message,
                        # Should we make different error codes for these?
                        'validation',
                        event
                    )
                    event_errors.append(event_error)
                    # If error_writer is configured, send this
                    # EventError to it.
                    if self.error_writer:
                        self.error_writer.send(event_error)

        if callback:
            callback(event_errors)
        else:
            return event_errors

    def start(self):
        """
        Starts the Tornado application ioloop.
        """
        tornado.ioloop.IOLoop.current().start()


class EventHandler(
    statsd.RequestMetricsMixin,
    tornado.web.RequestHandler
):

    @tornado.gen.coroutine
    def post(self):
        """
        events_string json string is read in from POST body.
        It can be a single event object or a list of event objects.
        They will be asynchronously parsed and validated, and then
        written to configured EventLogging writers.  'topic'
        must be set in each event's meta data.

        Reponses:
        - 201 if all events are accepted.
        - 207 if some but not all events are accepted.
        - 400 if no events are accepted.

        In case of any errored events, those events will be in the response
        body as a JSON list of the form:
        [{'event': {...}, 'error': 'String Error Message'}, ... ]

        # TODO: Use EventError and configure an error writer like
          eventlogging-processor?
        """
        response_body = None
        if self.request.headers['Content-Type'] == 'application/json':
            try:
                if self.request.body:

                    # Load the json body into Event objects.
                    events = yield tornado.gen.Task(
                        apply_safe, Event.factory, {'data': self.request.body}
                    )

                    # If we were only given a single event in the json,
                    # convert it to a list so the rest of the code just works.
                    if isinstance(events, dict):
                        events = [events]

                    # Process and validate all events.
                    event_errors = yield tornado.gen.Task(
                        self.application.handle_events, events
                    )
                    events_count = len(events)
                    event_errors_count = len(event_errors)

                    # If all events were accepted, then return 201
                    if event_errors_count == 0:
                        response_code = 201
                        response_text = 'All %s events were accepted.' % (
                            events_count
                        )
                    else:
                        # Else if all events failed validation
                        # return 400 and list of EventErrors.
                        if events_count == event_errors_count:
                            response_code = 400
                            response_text = ('0 out of %s events were '
                                             'accepted.') % events_count
                        # Else at least 1 event failed validation.
                        # Return 207 and the list of list of EventErrors.
                        else:
                            response_code = 207
                            response_text = ('%s out of %s events '
                                             'were accepted.') % (
                                events_count - event_errors_count,
                                events_count
                            )
                        response_body = json.dumps(event_errors)
                else:
                    response_code = 400
                    response_text = 'Must provide body in request.'

            except UnicodeError as e:
                response_code = 400
                response_text = 'UnicodeError while utf-8 decoding '
                'POST body: %s' % e

        else:
            response_code = 400
            response_text = 'Cannot produce messages of type %s.' % \
                self.request.headers['Content-Type']

        # Log error if not a 2xx response.
        if not (200 <= response_code <= 299):
            logging.error(response_text)

        self.set_status(response_code, response_text)
        if response_body:
            self.write(response_body)


class TopicConfigHandler(
    statsd.RequestMetricsMixin,
    tornado.web.RequestHandler
):
    def get(self):
        # Default to returning the object as JSON,
        # unless YAML is explicitly asked for.
        response_content_type = get_response_content_type(self.request.headers)
        should_write_yaml = 'yaml' in response_content_type

        topic_config = get_topic_config()
        if should_write_yaml:
            topic_config = to_yaml(topic_config)

        self.set_status(200)
        self.set_header('Content-Type', response_content_type)
        self.write(topic_config)


class SchemasHandler(
    statsd.RequestMetricsMixin,
    tornado.web.RequestHandler
):
    def get(self, schema_uri=None):
        """
        If schema_uri is none, this will write all schemas to the client.
        Else it will try to look up schema_uri in the schema_cache
        and return it to the client. If not found, or if schema_uri is
        invalid, this will return 404.

        Default behavior is to write to client as JSON, unless
        Accept header is application/x-yaml.

        Arguments:
            *schema_uri string
        """
        # Default to returning the object as JSON,
        # unless YAML is explicitly asked for.
        response_content_type = get_response_content_type(self.request.headers)
        should_write_yaml = 'yaml' in response_content_type

        # If we weren't given a specific schema_uri to look up,
        # then return the whole schema_cache.
        if not schema_uri:
            schema_uris = get_cached_schema_uris()

            if should_write_yaml:
                schema_uris = to_yaml(schema_uris)
            else:
                # Have to manually encode as json, Tornado
                # won't let us write lists as bodies directly.
                schema_uris = json.dumps(schema_uris)

            self.set_status(200)
            self.set_header('Content-Type', response_content_type)
            self.write(schema_uris)

        # Else attempt to look up this schema_uri in the schema_cache.
        else:
            schema = None
            try:
                scid = scid_from_uri(
                    schema_uri, default_to_latest_revision=True
                )
                if scid:
                    schema = get_schema(scid, remote_enabled=False)
            except (SchemaError, ValidationError):
                pass

            if schema:
                if should_write_yaml:
                    schema = to_yaml(schema)
                self.set_header('Content-Type', response_content_type)
                self.set_status(200)
                self.write(schema)
            else:
                self.set_status(404)
                self.write("No schema exists at schema_uri %s\n" % schema_uri)


class SpecHandler(tornado.web.RequestHandler):
    def get(self):
        # only respond to ?spec
        if self.request.query == 'spec':
            self.set_status(200)
            self.write(swagger_spec)
        else:
            self.set_status(404)


def to_yaml(o):
    """Safe yaml dump o with default_flow_style=False."""
    return yaml.safe_dump(o, default_flow_style=False)


def get_response_content_type(request_headers):
    """
    Examines request headers and to see if the client Accepts yaml.
    Returns the value you should use for the response Content-Type.
    This assumes the default response is json.
    """
    headers = ['application/x-yaml', 'application/yaml']
    if ('Accept' in request_headers and
            request_headers['Accept'] in headers):
        return 'application/x-yaml; charset=UTF-8'
    else:
        return 'application/json; charset=UTF-8'


def append_spec_test_topic_and_schema(overwrite=False):
    """
    Augments the topic config and schema cache with a test
    topic config and test schema used to automate testing
    via the swagger spec's x-amples.  If overwrite is False,
    an exception will be raised if the spec test topic or schema
    are already present in the topic config or schema cache,
    so make you don't try to configure a topic or schema with
    a conflicting name.  This is the default behavior.

    :param overwrite: boolean
    :raises :exc:`Exception`:
    """
    if not overwrite:
        # Error and die if someone's provided topic config or
        # schemas already have the spec test topic/schema.
        if is_topic_configured(spec_test_topic):
            raise Exception(
                'Topic \'%s\' cannot be present in your topic config. It is '
                'reserved for eventlogging-service swagger spec testing.' %
                spec_test_topic
            )
        if is_schema_cached(spec_test_scid):
            raise Exception(
                'Schema (%s,%s) cannot be present in your local schemas. It '
                'is reserved for eventlogging-service swagger spec testing.' %
                spec_test_scid
            )

    spec_test_topic_config = {
        spec_test_topic: {'schema_name': spec_test_scid[0]}
    }
    spec_test_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'title': 'Test Event Schema',
        'description': 'Schema used for simple tests',
        'properties': {
            'type': 'object',
            'test': {'type': 'string'},
            'meta': {
                'type': 'object',
                'properties': {
                    'domain': {'type': 'string'},
                    'dt': {'format': 'date-time', 'type': 'string'},
                    'id': {'type': 'string'},
                    'request_id': {'type': 'string'},
                    'schema_uri': {'type': 'string'},
                    'topic': {'type': 'string'},
                    'uri': {'format': 'uri', 'type': 'string'}
                },
                'required': ['topic', 'id'],
            }
        }
    }
    # Augment topic_config and schema_cache
    # with test topic and schema.
    update_topic_config(spec_test_topic_config)
    cache_schema(spec_test_scid, spec_test_schema)


def init_schemas_and_topic_config(
    topic_config_path,
    schemas_path
):
    """
    Calls init_topic_config and init_schema_cache and
    then append_spec_test_topic_and_schema() to augment
    the topic and schema configs to allow for automated
    testing of POST events via the swagger spec's
    x-amples.

    :param topic_config_path: Path to topic config YAML file
    :param schemas_path: Path to local schema repository directory
    """
    init_topic_config(topic_config_path)
    init_schema_cache(schemas_path)
    append_spec_test_topic_and_schema()
