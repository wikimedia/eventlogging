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

from . import ValidationError, SchemaError  # these are int __init__.py
from .compat import json
from .factory import apply_safe, get_writer
from .schema import (
    validate, get_schema, schema_name_from_event, id_from_event,
    create_event_error
)
from .topic import (
    get_topic_config, scid_for_topic, schema_for_topic, topic_from_event,
    TopicNotConfigured, TopicNotFound,
)


class EventLoggingService(tornado.web.Application):
    """
    EventLogging HTTP Produce Service\n

    Routes:

      POST /v1/events
      GET  /v1/topics/{topic}
      GET  /v1/topics
      GET  /v1/schemas/{schema_name}[/{schema_revision}]

    NOTE: If you are writing events to Kafka, you should make sure that you
    configure your kafka writer with async=False.  This will allow you
    to be sure that a 201 response will not be returned to the HTTP client
    until your event is ACKed by Kafka.
    """

    def __init__(self, writer_uris, error_writer_uri=None):
        """
        :param writer_uris: A list of EventLogging writer_uris.  Each valid
        event will be written to each of these writers.

        :param error_writer_uri: If configured, EventErrors will be written
        to this writer.
        """

        routes = [
            # POST /v1/events
            (r"/v1/events", EventHandler),

            # GET /v1/topics/{topic}
            (r"/v1/topics/([\w\d\-_]+)", TopicHandler),

            # GET /v1/topics
            (r"/v1/topics", TopicConfigHandler),

            # GET /v1/schemas/{schema_name}[/{schema_revision}]
            (r"/v1/schemas/([\w\d\-_]+)(?:/(\d+))?", SchemaHandler),
        ]

        super(EventLoggingService, self).__init__(routes)

        # Valid events will be sent to each of these writers.
        # Save the writer_uris as keys so that we can restart
        # failed writers.
        self.writers = {}
        for uri in writer_uris:
            self.writers[uri] = get_writer(uri)
            logging.info('Publishing valid JSON events to %s.', uri)

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

    def decode_json(self, s, callback=None):
        """
        Loads the string into an object.  If the top level
        object is a dict, it will be wrapped in a list.
        The result of this function should be a list of events.

        :param s: json string
        """
        events = json.loads(s.decode('utf-8'))
        if isinstance(events, dict):
            events = [events]
        if callback:
            callback(events)
        else:
            return events

    def process_event(self, event):
        """
        Validate the event using the schema configured for it's topic.
        A valid event will be sent to the configured writers.

        Returns True on success, otherwise some Exception will be thrown.

        """
        topic = topic_from_event(event)
        schema_name, revision = scid_for_topic(topic)

        # Set topic, schema, and revision on event metadata subobject.
        if 'meta' in event:
            event['meta']['schema'] = schema_name
            event['meta']['revision'] = revision
            # meta style events don't use encapsulate
            encapsulate = False

        # Else meta data is top level (EventCapsule style).
        else:
            event['schema'] = schema_name
            event['revision'] = revision
            # EventCapsule style meta data does use encapsulate
            encapsulate = True

        validate(event, encapsulate=encapsulate)
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
        # This will add schema and revision to the event
        # based on topic config.

        event_errors = []
        for event in events:
            error_message = None

            try:
                self.process_event(event)

            except TopicNotConfigured as e:
                error_message = str(e)

            except TopicNotFound as e:
                error_message = 'Could not get topic from event %s. %s' % (
                    id_from_event(event), e
                )

            except SchemaError as e:
                error_message = 'Could not find schema for provided topic ' \
                    'in event %s. %s' % (id_from_event, e)

            except ValidationError as e:
                error_message = 'Failed validating event %s of schema ' \
                    '%s. %s' % (
                        id_from_event(event),
                        schema_name_from_event(event),
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

    def start(self, port=8085, num_processes=1):
        """
        Starts this application listening on port
        with num_processes.
        """
        server = tornado.httpserver.HTTPServer(self)
        server.bind(port)
        server.start(int(num_processes))
        tornado.ioloop.IOLoop.current().start()


class EventHandler(tornado.web.RequestHandler):

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
                    # decode the json string into a list
                    events = yield tornado.gen.Task(
                        self.application.decode_json,
                        self.request.body
                    )
                    # process and validate events
                    event_errors = yield tornado.gen.Task(
                        self.application.handle_events,
                        events
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


class TopicHandler(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def get(self, topic):
        try:
            schema = yield tornado.gen.Task(
                apply_safe, schema_for_topic, {'topic': topic}
            )
        except TopicNotConfigured as e:
            self.set_status(404, str(e))
        else:
            self.set_status(200)
            self.write(schema)


class SchemaHandler(tornado.web.RequestHandler):

    @tornado.gen.coroutine
    def get(self, schema_name, revision):
        """
        Finds and returns schema for provided scid.
        """
        try:
            if not revision:
                revision = 0
            else:
                revision = int(revision)

            scid = (schema_name, revision)

            schema = yield tornado.gen.Task(
                apply_safe, get_schema, {'scid': scid}
            )
        except SchemaError as e:
            response_text = 'Schema %s,%s not found. %s' % (
                schema_name, revision, e
            )
            logging.error(response_text)
            self.set_status(404, response_text)
        else:
            self.set_status(200)
            self.write(schema)


class TopicConfigHandler(tornado.web.RequestHandler):

    def get(self):
        self.set_status(200)
        self.write(get_topic_config())
