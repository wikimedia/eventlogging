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

from .schema import get_schema, get_from_meta

__all__ = (
    'init_topic_config', 'scid_for_topic', 'schema_for_topic',
    'topic_from_event', 'TopicNotConfigured'
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


def scid_for_topic(topic):
    """
    Get the scid for a topic out of topic_config.
    If revision is not set, the latest revision (0)
    will be assumed.
    """
    if topic not in topic_config:
        raise TopicNotConfigured("Topic %s not configured" % topic)

    return (
        topic_config[topic]['schema'],
        # Default to revision 0 if not set in topic config.
        # (0 signifies latest revision.)
        topic_config[topic].get('revision', 0)
    )


def schema_for_topic(topic, encapsulate=False):
    """
    Returns the schema associated with this topic.
    """
    return get_schema(
        scid_for_topic(topic),
        encapsulate=encapsulate
    )


def topic_from_event(event, topic_format=None, default=None):
    """
    Returns the topic for this event, or None if no topic is found.

    Given an event, this will return the topic to use..
    If topic_format is None, then the value of 'topic' in the event meta data
    will be used.
    Otherwise the event wil be formatted with topic_format.
    E.g.
      topic_format = 'eventlogging_{schema}'
        OR
      topic_format = 'eventlogging_{meta[schema]}'

      topic = topic_format.format(**event)

    :raises :exc:`TopicNotFound` if topic cannot be extracted from event
    """
    topic = default
    if not topic_format:
        topic = get_from_meta('topic', event)
        if not topic:
            raise TopicNotFound(
                'Could not extract topic from event meta data.'
            )
    else:
        # Interpolate topic from event.
        try:
            topic = topic_format.format(**event)
        except KeyError as e:
            raise TopicNotFound(
                'Could not interpolate topic from event with format '
                '\'%s\'. KeyError: %s' % (topic_format, e))

    return topic
