# -*- coding: utf-8 -*-
"""
  eventlogging unit tests
  ~~~~~~~~~~~~~~~~~~~~~~~

  This module contains tests for :module:`eventlogging.service`.
"""

import copy
from tornado.testing import AsyncHTTPTestCase
from tornado.testing import get_unused_port

import eventlogging
from eventlogging.service import (
    EventLoggingService, swagger_spec, append_spec_test_topic_and_schema,
    get_response_content_type
)
from eventlogging.schema import (
    get_schema, is_schema_cached, schema_uri_from_scid
)
from eventlogging.topic import schema_name_for_topic, TopicNotConfigured
from eventlogging.event import Event

import json
import yaml
import os
import tempfile
from .fixtures import SchemaTestMixin, TEST_META_SCHEMA_SCID


class TestEventLoggingService(SchemaTestMixin, AsyncHTTPTestCase):
    """
    Testing of EventLogging REST produce API.
    """
    def setUp(self):
        super(TestEventLoggingService, self).setUp()

    def get_app(self):
        writers = []
        application = EventLoggingService(writers, None, get_unused_port())
        return application

    def test_get_response_content_type(self):
        self.assertEqual(
            'application/json; charset=UTF-8',
            get_response_content_type({})
        )
        self.assertEqual(
            'application/json; charset=UTF-8',
            get_response_content_type({'Accept': 'blablabla'})
        )
        self.assertEqual(
            'application/x-yaml; charset=UTF-8',
            get_response_content_type({'Accept': 'application/yaml'})
        )
        self.assertEqual(
            'application/x-yaml; charset=UTF-8',
            get_response_content_type({'Accept': 'application/x-yaml'})
        )

    def test_spec(self):
        """
        Test ?spec returns swagger spec.
        """
        self.http_client.fetch(self.get_url('/?spec'),
                               self.stop, method="GET")
        response = self.wait()
        self.assertEqual(200, response.code)

        self.assertEqual

    def test_schemas(self):
        """
        Test /v1/schemas returns a list of schema_uris
        """
        schema_uri = schema_uri_from_scid(TEST_META_SCHEMA_SCID)
        self.http_client.fetch(self.get_url('/v1/schemas'),
                               self.stop, method="GET")
        response = self.wait()

        self.assertEqual(
            'application/json; charset=UTF-8',
            response.headers['Content-Type']
        )
        self.assertEqual(200, response.code)
        # Assert that a schema we know about is
        # in the returned  list of schema_uris.
        schema_uris = yaml.safe_load(response.body)
        self.assertTrue(schema_uri in schema_uris)

    def test_schemas_yaml(self):
        """
        Test /v1/schemas returns a list of schemas in yaml
        """
        schema_uri = schema_uri_from_scid(TEST_META_SCHEMA_SCID)

        # Request that schemas are returned to us as a yaml string
        headers = {'Accept': 'application/x-yaml'}

        self.http_client.fetch(self.get_url('/v1/schemas'),
                               self.stop, method="GET", headers=headers)
        response = self.wait()

        self.assertEqual(
            'application/x-yaml; charset=UTF-8',
            response.headers['Content-Type']
        )
        self.assertEqual(200, response.code)

        # Assert that a schema we know about is
        # in the returned  list of schema_uris.
        schema_uris = yaml.safe_load(response.body)
        self.assertTrue(schema_uri in schema_uris)

    def test_schemas_with_schema_uri(self):
        """
        Test /v1/schemas/TestMetaSchema/1 returns the proper schema
        """
        schema = get_schema(TEST_META_SCHEMA_SCID)

        self.http_client.fetch(
            self.get_url('/v1/schemas/%s/%s' % TEST_META_SCHEMA_SCID),
            self.stop, method="GET"
        )
        response = self.wait()

        self.assertEqual(
            'application/json; charset=UTF-8',
            response.headers['Content-Type']
        )
        self.assertEqual(200, response.code)

        # assert that the schema we got is like the one in schema_cache
        response_schema = yaml.safe_load(response.body)
        self.assertEqual(schema['title'], response_schema['title'])

    def test_schemas_with_schema_uri_yaml(self):
        """
        Test /v1/schemas/TestMetaSchema/1 returns the proper schema in yaml
        """
        schema = get_schema(TEST_META_SCHEMA_SCID)

        # Request that schemas are returned to us as a yaml string
        headers = {'Accept': 'application/x-yaml'}

        self.http_client.fetch(
            self.get_url('/v1/schemas/%s/%s' % TEST_META_SCHEMA_SCID),
            self.stop, method="GET", headers=headers
        )
        response = self.wait()

        self.assertEqual(
            'application/x-yaml; charset=UTF-8',
            response.headers['Content-Type']
        )
        self.assertEqual(200, response.code)

        # assert that the schema we got is like the one in schema_cache
        response_schema = yaml.safe_load(response.body)
        self.assertEqual(schema['title'], response_schema['title'])

    def test_schemas_with_schema_uri_latest_revision(self):
        """
        Test /v1/schemas/TestMetaSchema returns the latest schema
        """
        schema = get_schema(TEST_META_SCHEMA_SCID)

        self.http_client.fetch(
            self.get_url('/v1/schemas/%s' % TEST_META_SCHEMA_SCID[0]),
            self.stop, method="GET"
        )
        response = self.wait()

        self.assertEqual(
            'application/json; charset=UTF-8',
            response.headers['Content-Type']
        )
        self.assertEqual(200, response.code)

        # assert that the schema we got is like the one in schema_cache
        response_schema = yaml.safe_load(response.body)
        self.assertEqual(schema['title'], response_schema['title'])

    def test_event_post_spec_x_amples(self):
        """
        Test that the /v1/events POST x-amples
        can be used for doing monitoring test POSTs
        to /v1/events.
        """
        # This needs to be called if we want to be able to do the
        # swagger x-amples test POST to /v1/events.
        append_spec_test_topic_and_schema(overwrite=True)

        events = swagger_spec['paths']['/v1/events']['post']['x-amples'][0]['request']['body']  # noqa
        headers = {'Content-type': 'application/json'}
        self.http_client.fetch(self.get_url('/v1/events'),
                               self.stop, method="POST",
                               body=json.dumps(events), headers=headers)
        response = self.wait()
        self.assertEqual(201, response.code)

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

    def test_append_spec_test_topic_and_schema(self):
        # assert that the test spec topic and scid are not yet in the
        # topic config or schema cache.
        with self.assertRaises(TopicNotConfigured):
            schema_name_for_topic(eventlogging.service.spec_test_topic)

        self.assertFalse(is_schema_cached(eventlogging.service.spec_test_scid))

        append_spec_test_topic_and_schema(overwrite=False)
        # now the test spec topic and scid should exist
        self.assertEqual(
            eventlogging.service.spec_test_scid[0],
            schema_name_for_topic(eventlogging.service.spec_test_topic)
        )
        self.assertTrue(is_schema_cached(eventlogging.service.spec_test_scid))

        # with overwrite false, append_spec_test_topic_and_schema should
        # now raise an exception.
        with self.assertRaises(Exception):
            append_spec_test_topic_and_schema(overwrite=False)

        # but with overwrite True, all should be fine.
        append_spec_test_topic_and_schema(overwrite=True)


class TestEventLoggingServiceWithFileWriter(
    SchemaTestMixin, AsyncHTTPTestCase
):
    """
    Testing of EventLogging REST produce API actually writing to a temp file.
    A new temp file will be used for each test, and deleted in tearDown().
    """
    def setUp(self):
        super(TestEventLoggingServiceWithFileWriter, self).setUp()

    def tearDown(self):
        os.remove(self.temp_file_path)

    def get_app(self):
        (_, self.temp_file_path) = tempfile.mkstemp(
            prefix='eventlogging-service-test',
            text=True,
        )
        writers = ['file://' + self.temp_file_path]
        application = EventLoggingService(writers, None, get_unused_port())

        return application

    def event_from_temp_file(self):
        """
        Read the event(s) from the temp_file.
        """
        with open(self.temp_file_path, 'r') as f:
            event = Event.factory(f)
        return event

    def test_produce_valid_event_configured_topic(self):
        """
        Posting a valid event to a configured topic returns 201
        and is fully produced.
        """
        headers = {'Content-type': 'application/json'}
        body = json.dumps(self.event_with_meta)
        self.http_client.fetch(self.get_url('/v1/events'),
                               self.stop, method="POST",
                               body=body, headers=headers)
        response = self.wait()
        self.assertEqual(201, response.code)

        produced_event = self.event_from_temp_file()
        self.assertEqual(
            self.event_with_meta,
            produced_event
        )
