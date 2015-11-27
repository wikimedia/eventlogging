# -*- coding: utf-8 -*-
"""
  eventlogging unit tests
  ~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for :module:`eventlogging.service`.
"""

import copy
from tornado.testing import AsyncHTTPTestCase
from eventlogging.service import EventLoggingService

import json
from .fixtures import SchemaTestMixin


class TestEventLoggingService(SchemaTestMixin, AsyncHTTPTestCase):
    """
    Testing of EventLogging REST produce API.
    """
    def setUp(self):
        super(TestEventLoggingService, self).setUp()

    def get_app(self):
        writers = []
        application = EventLoggingService(writers)
        return application

    # Event Testing
    def test_event_post_topic_does_not_exist(self):
        """
        Posting to a topic that does not exists returns a 404
        """
        # The following two lines are equivalent to
        #   response = self.fetch('/')
        # but are shown in full here to demonstrate explicit use
        # of self.stop and self.wait.
        event = {
            'some': 'Blah',
            'meta': {
                'topic': 'badtopic',
                'schema_uri': 'DoesNotMatter/1'
            }
        }
        headers = {'Content-type': 'application/json'}
        self.http_client.fetch(self.get_url('/v1/events'),
                               self.stop, method="POST",
                               body=json.dumps(event), headers=headers)
        response = self.wait()
        self.assertEqual(400, response.code)
        self.assertTrue("Topic badtopic not configured" in str(response.body))

    def test_post_valid_event_configured_topic(self):
        """
        Posting a valid event to a configured topic returns 201
        """
        headers = {'Content-type': 'application/json'}
        body = json.dumps(self.event_with_meta)
        self.http_client.fetch(self.get_url('/v1/events'),
                               self.stop, method="POST",
                               body=body, headers=headers)
        response = self.wait()
        self.assertEqual(201, response.code)

    def test_post_valid_event_capsule_configured_topic(self):
        """
        Posting a valid EventCapsule style event
        to aconfigured topic returns 201
        """
        headers = {'Content-type': 'application/json'}
        body = json.dumps(self.event)
        self.http_client.fetch(self.get_url('/v1/events'),
                               self.stop, method="POST",
                               body=body, headers=headers)
        response = self.wait()
        self.assertEqual(201, response.code)

    def test_post_event_missing_required_field(self):
        """
        Posting an invalid event to a configured topic returns 400
        and meaningful message
        """
        headers = {'Content-type': 'application/json'}
        invalid_event = copy.deepcopy(self.event_with_meta)
        del invalid_event['required_field']
        body = json.dumps(invalid_event)
        self.http_client.fetch(self.get_url('/v1/events'),
                               self.stop, method="POST",
                               body=body, headers=headers)
        response = self.wait()
        self.assertEqual(400, response.code)
        self.assertTrue("Failed validating" in str(response.body))

    def test_post_event_missing_optional_field(self):

        headers = {'Content-type': 'application/json'}
        valid_event = copy.deepcopy(self.event_with_meta)
        del valid_event['optional_field']
        body = json.dumps(valid_event)
        self.http_client.fetch(self.get_url('/v1/events'),
                               self.stop, method="POST",
                               body=body, headers=headers)
        response = self.wait()
        self.assertEqual(201, response.code)

    def test_post_event_batch(self):

        headers = {'Content-type': 'application/json'}
        valid_eventA = copy.deepcopy(self.event_with_meta)
        valid_eventB = copy.deepcopy(self.event_with_meta)
        events = [valid_eventA, valid_eventB]
        body = json.dumps(events)
        self.http_client.fetch(self.get_url('/v1/events'),
                               self.stop, method="POST",
                               body=body, headers=headers)
        response = self.wait()
        self.assertEqual(201, response.code)

    def test_post_event_batch_one_invalid(self):

        headers = {'Content-type': 'application/json'}
        valid_eventA = copy.deepcopy(self.event_with_meta)
        valid_eventB = copy.deepcopy(self.event_with_meta)
        # this is supposed to be a string.
        valid_eventB['required_field'] = 123

        events = [valid_eventA, valid_eventB]
        body = json.dumps(events)
        self.http_client.fetch(self.get_url('/v1/events'),
                               self.stop, method="POST",
                               body=body, headers=headers)
        response = self.wait()
        self.assertEqual(207, response.code)
        event_errors = json.loads(response.body.decode('utf-8'))
        self.assertEqual('validation', event_errors[0]['event']['code'])

    def test_post_event_batch_all_invalid(self):

        headers = {'Content-type': 'application/json'}
        valid_eventA = copy.deepcopy(self.event_with_meta)
        valid_eventA['required_field'] = 123
        valid_eventB = copy.deepcopy(self.event_with_meta)
        valid_eventB['required_field'] = 456
        events = [valid_eventA, valid_eventB]
        body = json.dumps(events)
        self.http_client.fetch(self.get_url('/v1/events'),
                               self.stop, method="POST",
                               body=body, headers=headers)
        response = self.wait()
        self.assertEqual(400, response.code)
        event_errors = json.loads(response.body.decode('utf-8'))
        self.assertEqual('validation', event_errors[0]['event']['code'])
        self.assertEqual('validation', event_errors[1]['event']['code'])
