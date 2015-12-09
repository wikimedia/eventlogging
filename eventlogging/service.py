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

from . import ValidationError, SchemaError  # these are int __init__.py
from .compat import json
from .event import create_event_error, Event
from .factory import apply_safe, get_writer
from .schema import validate
from .topic import (
    get_topic_config, latest_scid_for_topic, schema_allowed_in_topic,
    schema_name_for_topic, TopicNotConfigured, TopicNotFound,
)

# These must be checked before we import sprockets because
# sprockets also sets the envrionment variables if they are not set.
# Set the default sprockets.mixins.statsd prefix.
os.environ.setdefault('STATSD_PREFIX', 'eventlogging.service')
# Don't report per host stats by default.
os.environ.setdefault('STATSD_USE_HOSTNAME', 'False')

from sprockets.mixins import statsd


class SchemaNotAllowedInTopic(Exception):
    pass


class EventLoggingService(tornado.web.Application):
    """
    EventLogging HTTP Produce Service\n

    Routes:

      POST /v1/events
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

            # GET /v1/topics
            (r"/v1/topics", TopicConfigHandler),
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

    def process_event(self, event):
        """
        Validate the event using the schema configured for it's topic.
        A valid event will be sent to the configured writers.

        Returns True on success, otherwise some Exception will be thrown.

        """
        topic = event.topic()
        scid = event.scid()

        if not scid:
            scid = latest_scid_for_topic(topic)
            # Fill in scid / schema_uri for this even
            # from the topic config
            logging.debug(
                '%s did not set scid/schema_uri. Setting to %s,%s '
                'for topic %s' % (event, scid[0], scid[1], topic)
            )
            event.set_scid(scid)
        else:
            # Make sure the provided event scid is allowed in this topic.
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
        self.set_status(200)
        self.write(get_topic_config())
