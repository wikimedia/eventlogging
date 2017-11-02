# -*- coding: utf-8 -*-
"""
  eventlogging.utils
  ~~~~~~~~~~~~~~~~~~

  This module contains generic routines that aren't associated with
  a particular function.

"""
from __future__ import unicode_literals

import copy
import datetime
import dateutil.parser
import logging
import re
import os
import socket
import sys
import threading
import traceback
import uuid
from ua_parser import user_agent_parser

from .compat import (
    items, monotonic_clock, urisplit, urlencode, parse_qsl,
    integer_types, string_types, long
)
from .factory import get_reader, cast_string
from logging.config import fileConfig


__all__ = ('EventConsumer', 'PeriodicThread', 'flatten', 'is_subset_dict',
           'setup_logging', 'unflatten', 'update_recursive',
           'uri_delete_query_item', 'uri_append_query_items', 'uri_force_raw',
           'parse_etcd_uri', 'datetime_from_uuid1', 'datetime_from_timestamp',
           'iso8601_from_timestamp')

# Regex extending uaparser's bot/spider detection, comes from
# Webrequest.java in refinery-source/core
bot_ua_pattern = re.compile('(.*(bot|spider|WordPress|AppEngine|AppleDictionar'
                            'yService|Python-urllib|python-requests|Google-HTT'
                            'P-Java-Client|[Ff]acebook|[Yy]ahoo|RockPeaks|http'
                            ').*|(goo wikipedia|MediaWikiCrawler-Google|wikiwi'
                            'x-bot|Java|curl|PHP|Faraday|HTTPC|Ruby|\.NET|Pyth'
                            'on|Apache|Scrapy|PycURL|libwww|Zend|wget|nodemw|W'
                            'inHttpRaw|Twisted|com\.eusoft|Lagotto|Peggo|Recuw'
                            'eb|check_http|Magnus|MLD|Jakarta|find-link|J\. Ri'
                            'ver|projectplan9|ADmantX|httpunit|LWP|iNaturalist'
                            '|WikiDemo|FSResearchIt|livedoor|Microsoft Monitor'
                            'ing|MediaWiki|User:|User_talk:|github|tools.wmfla'
                            'bs.org|Blackboard Safeassign|Damn Small XSS|\S+@'
                            '\S+\.[a-zA-Z]{2,3}).*)$')


class PeriodicThread(threading.Thread):
    """Represents a threaded job that runs repeatedly at a regular interval."""

    def __init__(self, interval, *args, **kwargs):
        self.interval = interval
        self.ready = threading.Event()
        self.stopping = threading.Event()
        self.logger = logging.getLogger('Log')
        super(PeriodicThread, self).__init__(*args, **kwargs)

    def run(self):
        while not self.stopping.is_set():
            try:
                # Run the target function. Check the clock before
                # and after to determine how long it took to run.
                time_start = monotonic_clock()
                self._Thread__target(*self._Thread__args,
                                     **self._Thread__kwargs)
                time_stop = monotonic_clock()

                run_duration = time_stop - time_start

                # Subtract the time it took the target function to run
                # from the desired run interval. The result is how long
                # we have to sleep before the next run.
                time_to_next_run = self.interval - run_duration
                self.logger.debug('Run duration of thread execution: %s',
                                  str(run_duration))
                if self.ready.wait(time_to_next_run):
                    # If the internal flag of `self.ready` was set, we were
                    # interrupted mid-nap to run immediately. But before we
                    # do, we reset the flag.
                    self.ready.clear()
            except Exception as e:
                trace = traceback.format_exc()
                self.logger.warn('Child thread exiting, exception %s', trace)
                raise e

    def stop(self):
        """Graceful stop: stop once the current iteration is complete."""
        self.stopping.set()
        self.logger.info('Stopping child thread gracefully')


def uri_delete_query_item(uri, key):
    """Delete a key-value pair (specified by key) from a URI's query string."""
    def repl(match):
        separator, trailing_ampersand = match.groups()
        return separator if trailing_ampersand else ''
    return re.sub('([?&])%s=[^&]*(&?)' % re.escape(key), repl, uri)


def uri_append_query_items(uri, params):
    """
    Appends uri with the dict params as key=value pairs using
    urlencode and returns the result.
    """
    return "{0}{1}{2}".format(
        uri,
        '&' if urisplit(uri).query else '?',
        urlencode(params)
    )


def uri_force_raw(uri):
    """
    Returns a uri that sets raw=True as a query param if it isn't already set.
    """
    if 'raw=True' not in uri:
        return uri_append_query_items(uri, {'raw': True})
    else:
        return uri


def is_subset_dict(a, b):
    """True if every key-value pair in `a` is also in `b`.
    Values in `a` which are themselves dictionaries are tested
    by recursively calling :func:`is_subset_dict`."""
    for key, a_value in items(a):
        try:
            b_value = b[key]
        except KeyError:
            return False
        if isinstance(a_value, dict) and isinstance(b_value, dict):
            if not is_subset_dict(a_value, b_value):
                return False
        elif a_value != b_value:
            return False
    return True


def update_recursive(d, other):
    """Recursively update a dict with items from another dict."""
    for key, val in items(other):
        if isinstance(val, dict):
            val = update_recursive(d.get(key, {}), val)
        d[key] = val
    return d


def flatten(d, sep='_', f=None):
    """Collapse a nested dictionary. `f` specifies an optional mapping
    function to apply to each key, value pair. This function is the inverse
    of :func:`unflatten`."""
    flat = []
    for k, v in items(d):
        if f is not None:
            (k, v) = f((k, v))
        if isinstance(v, dict):
            nested = items(flatten(v, sep, f))
            flat.extend((k + sep + nk, nv) for nk, nv in nested)
        else:
            flat.append((k, v))
    return dict(flat)


def unflatten(d, sep='_', f=None):
    """Expand a flattened dictionary. Keys containing `sep` are split into
    nested key selectors. `f` specifies an optional mapping function to apply
    to each key-value pair. This function is the inverse of :func:`flatten`."""
    unflat = {}
    for k, v in items(d):
        if f is not None:
            (k, v) = f((k, v))
        while sep in k:
            k, nested_k = k.split(sep, 1)
            v = {nested_k: v}
        if isinstance(v, dict):
            v = unflatten(v, sep)
        update_recursive(unflat, {k: v})
    return unflat


class EventConsumer(object):
    """An EventLogging consumer API for standalone scripts.

    .. code-block::

       event_stream = eventlogging.EventConsumer('tcp://localhost:8600')
       for event in event_stream.filter(schema='NavigationTiming'):
           print(event)

    """

    def __init__(self, url):
        self.url = url
        self.conditions = {}

    def filter(self, **conditions):
        """Return a copy of this consumer that will filter events based
        on conditions expressed as keyword arguments."""
        update_recursive(conditions, self.conditions)
        filtered = copy.copy(self)
        filtered.conditions = conditions
        return filtered

    def __iter__(self):
        """Iterate events matching the filter."""
        for event in get_reader(self.url):
            if is_subset_dict(self.conditions, event):
                yield event


def parse_etcd_uri(etcd_uri):
    """
    Parses an eventlogging formed URI and returns a kwargs dict suitable
    for passing to etcd.client.Client().
    """
    # etcd_uri should look like:
    # http(s)://hostA:1234,hostB:2345?allow_reconnect=True ...
    parts = urisplit(etcd_uri)
    etcd_kwargs = {
        k: cast_string(v) for k, v in
        items(dict(parse_qsl(parts.query)))
    }

    etcd_kwargs['protocol'] = parts.scheme
    # Convert the host part of uri into
    # a tuple of the form:
    # (('hostA', 1234), ('hostB', 1234))
    etcd_kwargs['host'] = tuple([
        (h.split(':')[0], int(h.split(':')[1]))
        for h in parts.netloc.split(',')
    ])
    return etcd_kwargs


def kafka_ids(identity=None):
    """
    Returns a tuple of (client_id, group_id) based on the eventlogging
    identity.  These are useful for passing to Kafka clients.
    If identity is None, 'eventlogging' + a uuid1 will be used.
    """
    # identity is used to define the consumer group.id and the prefix of
    # the client.id. If identity is not given create a default unique
    # one. This ensures we don't accidentally put consumers to the same group.
    # Explicitly specify identity to launch consumers in the same consumer
    # group.
    identity = identity if identity else 'eventlogging-{0}'.format(
        str(uuid.uuid1())
    )
    group_id = identity
    client_id = '{0}-{1}.{2}'.format(group_id, socket.getfqdn(), os.getpid())
    return (client_id, group_id)


def datetime_from_uuid1(u):
    """
    Extracts a datetime timestamp from a uuid1.
    """
    return datetime.datetime.fromtimestamp(
        (u.time - long(0x01b21dd213814000))*100/1e9
    )


def datetime_from_timestamp(t):
    """
    Returns a datetime.datetime instance from
    a timestamp.
    :param t: int, long or float timestamp, OR a string timestamp parseable
              by dateutil.parser.parse.
    """
    if isinstance(t, float):
        dt = datetime.datetime.fromtimestamp(t)
    elif isinstance(t, integer_types):
        # try to parse this as seconds
        try:
            dt = datetime.datetime.fromtimestamp(t)
        except ValueError:
            # try to parse this as milliseconds
            dt = datetime.datetime.fromtimestamp(t/1000.0)
    elif isinstance(t, string_types):
        dt = dateutil.parser.parse(t)
    else:
        raise RuntimeError(
            "Could not parse datetime from timestamp %s. "
            "%s is not a datetime parseable type" % (t, type(t))
        )
    return dt


def iso8601_from_timestamp(t):
    return datetime_from_timestamp(t).isoformat()


def setup_logging(config_file=None):
    if config_file:
        fileConfig(config_file)
    else:
        eventlogging_log_level = getattr(
            logging, os.environ.get('LOG_LEVEL', 'INFO')
        )
        logging.basicConfig(
            stream=sys.stderr,
            level=eventlogging_log_level,
            format='%(asctime)s [%(process)s] (%(threadName)-10s) %(message)s')

        # Set module logging level to INFO, DEBUG is too noisy.
        logging.getLogger("kafka").setLevel(logging.INFO)
        logging.getLogger("kazoo").setLevel(logging.INFO)


def parse_ua(user_agent):
    """
    Returns a dict containing the parsed User Agent data
    from a request's UA string. Uses the following format:
    {
        "device_family": "Other",
        "browser_family": "IE",
        "browser_major": "11",
        "browser_major": "0",
        "os_family": "Windows Vista",
        "os_major": null,
        "os_minor": null,
        "wmf_app_version": "-"
    }

    App version in user agents is parsed as follows:
    WikipediaApp/5.3.1.1011 (iOS 10.0.2; Phone)
    "wmf_app_version":"5.3.1.1011"
    WikipediaApp/2.4.160-r-2016-10-14 (Android 4.4.2; Phone) Google Play
    "wmf_app_version":"2.4.160-r-2016-10-14"
    """
    parsed_ua = user_agent_parser.Parse(user_agent)
    formatted_ua = {}
    formatted_ua['device_family'] = parsed_ua['device']['family']
    formatted_ua['browser_family'] = parsed_ua['user_agent']['family']
    formatted_ua['browser_major'] = parsed_ua['user_agent']['major']
    formatted_ua['browser_minor'] = parsed_ua['user_agent']['minor']
    formatted_ua['os_family'] = parsed_ua['os']['family']
    formatted_ua['os_major'] = parsed_ua['os']['major']
    formatted_ua['os_minor'] = parsed_ua['os']['minor']
    # default wmf_app_version is '-'
    formatted_ua['wmf_app_version'] = '-'
    # is request a bot/spider?
    formatted_ua['is_bot'] = is_bot(formatted_ua['device_family'], user_agent)
    # does the request come from MediaWiki?
    formatted_ua['is_mediawiki'] = is_mediawiki(user_agent)
    app_ua = 'WikipediaApp/'

    if app_ua in user_agent:
        items = user_agent.split()
        version = items[0].split("/")[1]
        formatted_ua['wmf_app_version'] = version

    # escape json so it doesn't cause problems when validating
    # to string (per capsule definition)
    return formatted_ua


def is_bot(device_family, user_agent):
    """
    Tests the raw user agent string against a bot regular expression
    if uaparser isn't already marking it as a spider
    """
    if device_family == 'Spider':
        return True
    elif device_family == 'Other':
        ua_string = user_agent.strip('"')
        return bool(bot_ua_pattern.match(ua_string))
    return False


def is_mediawiki(user_agent):
    """
    Checks if the user_agent comes from a MediaWiki backend, in order
    to properly tag it
    """
    return 'mediawiki' in user_agent.lower()
