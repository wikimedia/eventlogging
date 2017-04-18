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
import re

from .schema import get_latest_schema_revision

__all__ = (
    'get_topic_config', 'is_topic_configured', 'init_topic_config', 'latest_scid_for_topic',
    'schema_allowed_in_topic', 'schema_name_for_topic', 'TopicNotConfigured',
    'TopicNotFound', 'update_topic_config'
)

topic_config = {}
# Regex-based lookup is expensive, so we cache the result of regex lookups.
# The dictionary contains a mapping from the topic name to the topic config object.
topic_lookup_cache = {}


class TopicNotConfigured(Exception):
    pass


class TopicNotFound(Exception):
    pass


def find_topic_config_by_regex(topic):
    """
    Finds the topic configuration by the regex-based topic configs.
    If 2 or more regex configs match a single topic the result would
    be non-deterministic.

    :param topic: the name of the topic
    :return: the topic config object
    """
    for topic_spec_name, topic_spec in topic_config.items():
        if re.match('^\/.+\/$', topic_spec_name) and re.match(topic_spec_name[1:-1], topic):
            topic_lookup_cache[topic] = topic_spec
            return topic_spec['schema_name']
    topic_lookup_cache[topic] = False
    return None


def init_topic_config(config_file):
    """
    Clears topic_config and loads the topic config from a file.

    :param config_file: Path to topic config YAML file
    """
    topic_config.clear()
    topic_lookup_cache.clear()

    # Load the topic_config from the config file.
    with open(config_file) as f:
        topic_config.update(yaml.load(f))


def update_topic_config(c):
    """
    Given a single topic config c, updates this config
    into the global topic_config.
    """
    topic_config.update(c)


def get_topic_config():
    """
     Returns topic configuration object.
    """
    return topic_config


def is_topic_configured(topic):
    """
    Returns True if the topic configuration exists.
    """
    if topic in topic_config \
            or topic in topic_lookup_cache\
            and topic_lookup_cache[topic]:
        return True
    elif topic in topic_lookup_cache and topic_lookup_cache[topic] is False:
        return False
    else:
        return bool(find_topic_config_by_regex(topic))


def schema_name_for_topic(topic):
    """
    Returns the schema name that is allowed to be produced
    to this topic.

    :raises TopicNotConfigured: if topic is not in topic_config.
    """
    if topic in topic_config:
        return topic_config[topic]['schema_name']
    elif topic in topic_lookup_cache:
        if topic_lookup_cache[topic]:
            return topic_lookup_cache[topic]['schema_name']
        else:
            raise TopicNotConfigured("Topic %s not configured" % topic)
    else:
        conf = find_topic_config_by_regex(topic)
        if conf:
            return conf['schema_name']
        else:
            raise TopicNotConfigured("Topic %s not configured" % topic)


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
