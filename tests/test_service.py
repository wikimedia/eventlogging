# -*- coding: utf-8 -*-
"""
  eventlogging unit tests
  ~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for :module:`eventlogging.service`.
"""

import copy
from tornado.testing import AsyncHTTPTestCase
from eventlogging import EventLoggingService

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
            }
        }
        headers = {'Content-type': 'application/json'}
        self.http_client.fetch(self.get_url('/v1/events'),
                               self.stop, method="POST",
                               body=json.dumps(event), headers=headers)
        response = self.wait()
        self.assertEqual(404, response.code)

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
        self.assertTrue("'required_field' is a required property"
                        in response.reason)

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

    # Topic Testing
    def test_list_topics(self):
        """
        Topics url should return topics available as json object
        """
        self.http_client.fetch(
            self.get_url('/v1/topics'),
            self.stop,
            method="GET"
        )
        response = self.wait()
        self.assertEqual(200, response.code)
        self.assertTrue('"topic_with_meta": {"schema": "TestMetaSchema"}'
                        in str(response.body))

    def test_get_topic_happy_case(self):
        """
        Get an existing topic
        """
        self.http_client.fetch(
            self.get_url('/v1/topics/topic_with_meta'),
            self.stop, method="GET"
        )
        response = self.wait()
        self.assertEqual(200, response.code)
        self.assertTrue(
            'Test Schema with Meta Data in Subobject' in str(response.body)
        )

    def test_get_topic_does_not_exist(self):
        """
        Geting a non existing topic returns an error
        """
        self.http_client.fetch(
            self.get_url('/v1/topics/bad_topic'),
            self.stop,
            method="GET"
        )
        response = self.wait()
        self.assertEqual(404, response.code)

    # Schema testing
    def test_get_schema_happy_case(self):
        headers = {'Content-type': 'application/json'}
        self.http_client.fetch(
            self.get_url('/v1/schemas/TestMetaSchema/1'),
            self.stop,
            method="GET",
            headers=headers
        )
        response = self.wait()
        self.assertEqual(200, response.code)

    def test_get_schema_no_version_happy_case(self):
        headers = {'Content-type': 'application/json'}
        self.http_client.fetch(
            self.get_url('/v1/schemas/TestMetaSchema'),
            self.stop,
            method="GET",
            headers=headers
        )
        response = self.wait()
        self.assertEqual(200, response.code)

    # These return 404 because the schema cannot be
    # retrieved from the http store and EventLogging just assumes
    # that this means the schema does not exist.
    def test_get_schema_http_failure(self):
        headers = {'Content-type': 'application/json'}
        self.http_client.fetch(
            self.get_url('/v1/schemas/TestMetaSchema/1234'),
            self.stop,
            method="GET",
            headers=headers
        )
        response = self.wait()
        self.assertEqual(404, response.code)

    def test_get_schema_http_no_revision(self):
        headers = {'Content-type': 'application/json'}
        self.http_client.fetch(
            self.get_url('/v1/schemas/BadSchema'),
            self.stop,
            method="GET",
            headers=headers
        )
        response = self.wait()
        self.assertEqual(404, response.code)
