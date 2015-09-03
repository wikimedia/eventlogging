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

from _codecs import *  # For UnicodeError
import logging

from . import ValidationError, SchemaError  # these are int __init__.py
from .compat import json
from .factory import apply_safe, get_writer
from .schema import validate, get_schema, schema_name_from_event
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

    def __init__(self, writer_uris):
        """
        :param writer_uris: A list of EventLogging writer_uris.  Each valid
        event will be written to each of these writers.
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

    def send(self, event):
        """Send the event to configured eventlogging writers."""
        for uri in self.writers.keys():
            w = self.writers[uri]
            try:
                result = w.send(event)
            # If the writer coroutine has stopped (likley due to
            # an error during the previous send()), attempt to
            # recreate the writer now.
            except StopIteration as e:
                logging.error(
                    "Writer %s has stopped.  Attempting to restart." % uri
                )
                w = get_writer(uri)
                self.writers[uri] = w
                w.send(event)

    def process_json(
        self,
        event_string,
        callback=None
    ):
        """
        Parse the event_string as json and validate it
        using the schema configured for this event's topic.
        If valid, the event will be sent to all configured writers.

        If callback is set, then it is called with the
        parsed and validated event object, otherwise
        the event object is just returned.
        """

        # This will add schema and revision to the event
        # based on topic config.
        event = json.loads(event_string)
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

        # No encapsulation allowed with HTTP service (yet).
        validate(event, encapsulate=encapsulate)

        # Send this processed event to all configured writers
        # This will block until each writer finishes writing this event.
        self.send(event)

        if callback:
            callback(event)
        else:
            return event

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
        event_string json string is read in from POST body
        and then asynchronously parsed and validated, and then
        written to configured EventLogging writers.  'topic'
        must be set in the event meta data.
        """
        if self.request.headers['Content-Type'] == 'application/json':
            try:
                body = self.request.body.decode('utf-8')
                if body:
                    event = yield tornado.gen.Task(
                        self.application.process_json,
                        body
                    )
                    response_code = 201
                    response_text = '%s event on topic %s accepted. ' % (
                        schema_name_from_event(event), topic_from_event(event)
                    )
                else:
                    response_code = 400
                    response_text = 'Must provide body in request.'
            except UnicodeError as e:
                response_code = 400
                response_text = 'UnicodeError while utf-8 decoding '
                'POST body: %s' % e
            except TopicNotConfigured as e:
                response_code = 404
                response_text = str(e)
            except TopicNotFound as e:
                response_code = 400
                response_text = 'Must provide topic. %s' % e
            except IncorrectSerialization as e:
                response_code = 400
                response_text = e.message
            except ValidationError as e:
                response_code = 400
                response_text = 'Unable to validate event. ' + e.message
            except SchemaError as e:
                response_code = 500
                response_text = 'Could not find schema for provided '
                'topic. %s' % e

        else:
            response_code = 400
            response_text = 'Cannot produce messages of type %s.' % \
                self.request.headers['Content-Type']

        # Log error if not a 2xx response.
        if not (200 <= response_code <= 299):
            logging.error(response_text)

        self.set_status(response_code, response_text)


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


class IncorrectSerialization(Exception):
    pass
