# -*- coding: utf-8 -*-
"""
  eventlogging.topic
  ~~~~~~~~~~~~~~~~~~~

  This module implements schema lookup via topic configuration.
  Topic configuration specifies that only a certain schema can
  be used for any given topic.

  You must first call init_topic_config() if you want to use any
  of the functions in this module.

"""

import yaml

from .schema import get_latest_schema_revision

__all__ = (
    'init_topic_config', 'latest_scid_for_topic', 'schema_allowed_in_topic',
    'schema_name_for_topic', 'TopicNotConfigured', 'TopicNotFound'
)

topic_config = {}


class TopicNotConfigured(Exception):
    pass


class TopicNotFound(Exception):
    pass


def init_topic_config(config_file):
    """
    Clears topic_config and loads the topic config from a file.
    """
    topic_config.clear()

    # Load the topic_config from the config file.
    with open(config_file) as f:
        topic_config.update(yaml.load(f))


def get_topic_config():
    """
    Returns topic configuration object.
    """
    return topic_config


def schema_name_for_topic(topic):
    """
    Returns the schema name that is allowed to be produced
    to this topic.

    :raises TopicNotConfigured: if topic is not in topic_config.
    """
    if topic not in topic_config:
        raise TopicNotConfigured("Topic %s not configured" % topic)

    return topic_config[topic]['schema_name']


def latest_scid_for_topic(topic):
    """
    Returns the latest scid for the schema that is allowed to be produced
    to this topic.
    """
    name = schema_name_for_topic(topic)
    revision = get_latest_schema_revision(name)
    return (name, revision)


def schema_allowed_in_topic(schema_name, topic):
    """
    Returns boolean if this schema is allowed to be produced to topic
    based on topic_config.
    """
    return (schema_name == schema_name_for_topic(topic))
