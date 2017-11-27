# -*- coding: utf-8 -*-
"""
  eventlogging.parse
  ~~~~~~~~~~~~~~~~~~

  This module provides a scanf-like parser for raw log lines.

  The format specifiers hew closely to those accepted by varnishncsa.
  See the `varnishncsa documentation <https://www.varnish-cache.org
  /docs/trunk/reference/varnishncsa.html>`_ for details.

  Field specifiers
  ================

  +--------+-----------------------------+
  | Symbol | Field                       |
  +========+=============================+
  |   %o   | Omit space-delimited string |
  +--------+-----------------------------+
  |   %j   | JSON event object           |
  +--------+-----------------------------+
  |   %q   | Query-string-encoded JSON   |
  +--------+-----------------------------+
  |   %t   | Timestamp in NCSA format    |
  +--------+-----------------------------+
  |   %D   | Timestamp in ISO-8601 format|
  +--------+-----------------------------+
  |   %u   | User agent to be parsed     |
  +--------+-----------------------------+
  | %{..}i | Tab-delimited string        |
  +--------+-----------------------------+
  | %{..}s | Space-delimited string      |
  +--------+-----------------------------+
  | %{..}d | Integer                     |
  +--------+-----------------------------+

   '..' is the desired property name for the capturing group.

"""
from __future__ import division, unicode_literals

import calendar
import re
import time
import uuid

from .compat import json, unquote_plus, uuid5
from .event import Event
from .utils import parse_ua, iso8601_from_timestamp

__all__ = (
    'LogParser', 'ncsa_to_unix',
    'ncsa_utcnow', 'capsule_uuid',
)

# Format string (as would be passed to `strftime`) for timestamps in
# NCSA Common Log Format.
NCSA_FORMAT = '%Y-%m-%dT%H:%M:%S'

# Formats event capsule objects into URLs using the combination of
# origin hostname, sequence ID, and timestamp. This combination is
# guaranteed to be unique. Example::
#
#   event://cp1054.eqiad.wmnet/?seqId=438763&dt=2013-01-21T18:10:34
#
EVENTLOGGING_URL_FORMAT = (
    'event://%(recvFrom)s/?seqId=%(seqId)s&dt=%(dt)s'
)


def capsule_uuid(capsule):
    """Generate a UUID for a capsule object.

    Gets a unique URI for the capsule using `EVENTLOGGING_URL_FORMAT`
    and uses it to generate a UUID5 in the URL namespace.

    ..seealso:: `RFC 4122 <https://www.ietf.org/rfc/rfc4122.txt>`_.

    :param capsule: A capsule object (or any dictionary that defines
      `recvFrom`, `seqId`, and `dt`).

    """
    uuid_fields = {
        'recvFrom': capsule.get('recvFrom'),
        'seqId': capsule.get('seqId'),
        # TODO: remove this timestamp default as part of T179625
        'dt': capsule.get('dt', iso8601_from_timestamp(
            capsule.get('timestamp', time.time())
        ))
    }

    id = uuid5(uuid.NAMESPACE_URL, EVENTLOGGING_URL_FORMAT % uuid_fields)
    return '%032x' % id.int


def ncsa_to_unix(ncsa_ts):
    """Converts an NCSA Common Log Format timestamp to an integer
    timestamp representing the number of seconds since UNIX epoch UTC.

    :param ncsa_ts: Timestamp in NCSA format.
    """
    return calendar.timegm(time.strptime(ncsa_ts, NCSA_FORMAT))


def ncsa_utcnow():
    """Gets the current UTC date and time in NCSA Common Log Format"""
    return time.strftime(NCSA_FORMAT, time.gmtime())


def decode_qson(qson):
    """Decodes a QSON (query-string-encoded JSON) object.
    :param qs: Query string.
    """
    return json.loads(unquote_plus(qson.strip('?;')))


class LogParser(object):
    """Parses raw varnish/MediaWiki log lines into encapsulated events."""

    def __init__(self, format):
        """Constructor.

        :param format: Format string.
        """
        self.format = format

        # A mapping of format specifiers (%d, %i, etc.)
        # to a tuple of (regexp, caster).
        self.format_specifiers = {
            'd': (r'(?P<%s>\d+)', int),
            'i': (r'(?P<%s>[^\t]+)', str),
            'j': (r'(?P<capsule>\S+)', json.loads),
            'q': (r'(?P<capsule>\?\S+)', decode_qson),
            's': (r'(?P<%s>\S+)', str),
            't': (r'(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})',
                  ncsa_to_unix),
            'D': (r'(?P<dt>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})',
                  iso8601_from_timestamp),
            'u': (r'(?P<userAgent>[^\t]+)', parse_ua),
            # Set caster to None for the ignore/omit format specifier
            # so the corresponding value doesn't end up in the parsed event
            'o': (r'(?P<omit>\S+)', None),
        }

        # Field casters, ordered by the relevant field's position in
        # format string.
        self.casters = []

        # Convert the format string to a regex that will match
        # the incoming lines into named groups and include them
        # into the parsed dict.

        # Space chars in format string should match any number of
        # space characters.
        format = re.sub(' ', r'\s+', format)

        # Pattern that converts from format specifiers (e.g. %t, %d, etc.)
        # into a regex will extract into a matched group key name.
        format_to_regex_pattern = '(?<!%%)%%({(\w+)})?([%s])' % (
            ''.join(self.format_specifiers.keys())
        )
        raw = re.sub(
            re.compile(format_to_regex_pattern), self._repl, format
        )
        self.re = re.compile(raw)

    def _repl(self, spec):
        """Replace a format specifier with its expanded regexp matcher
        and append its caster to the list. Called by :func:`re.sub`.
        """
        _, name, specifier = spec.groups()
        matcher, caster = self.format_specifiers[specifier]
        if name:
            matcher = matcher % name
        self.casters.append(caster)
        return matcher

    def parse(self, line):
        """Parse a log line into a map of field names / values."""
        match = self.re.match(line)
        if match is None:
            raise ValueError(self.re.pattern, line)

        # Dict of capture group name to matched value, e.g userAgent: "..."
        matches = match.groupdict()

        if 'capsule' not in matches.keys():
            raise ValueError(
                '\'capsule\' was not matched in line, but it is required',
                (self.re.pattern, line)
            )

        # Just the matched capture group names.
        keys = sorted(matches, key=match.start)

        # Build a dict of capture group names to caster functions.
        # This works because self.casters is a list of functions in the
        # same order of keys returned by match.groupdict.
        # Also filter out the casters where caster is None, so we omit it.
        caster_dict = dict([pair for pair in zip(keys, self.casters)
                            if pair[1]])

        # 'capsule' is a required format specifier.
        # Parse it out now as the main event.
        capsule = caster_dict['capsule'](matches['capsule'])
        # capsule at this point MUST be a dict
        if not isinstance(capsule, dict):
            raise ValueError(
                'capsule was successfully parsed, but not as an object.',
                capsule
            )

        # Apply caster functions to event 'capsule'
        # level fields that also have casters defined.
        for k in (set(capsule.keys()) & set(caster_dict.keys())):
            capsule[k] = caster_dict[k](capsule[k])

        # For other fields that have been parsed out of the raw event log line
        # Apply their specifier 'caster' functions.  Only use the data from
        # the raw line if and only if it is not already present in the
        # capsule data.
        # This is how we keep user submitted event data in the capsule even if
        # there is data for that field in the raw line.  E.g. if the user sent
        # an event capsule with a 'userAgent' field already in it, we will use
        # the user sent userAgent, not the one that might be parsed from the
        # raw log line.  Note that in this example, the userAgent sent in
        # the event capsule data will only be 'cast' (parsed) if the %u
        # speficier was used, and the caster to a 'userAgent' field is
        # defined. This is true for any field.  If it is not in the format
        # string, it will not be 'cast', but just used as is.
        parsed_fields_from_line = {
            k: f(matches[k]) for k, f in caster_dict.items()
            # skip the 'capsule' caster, since we already did it,
            # and also skip any casters that are already in the
            # capsule, since we already did those too.
            if k != 'capsule' and k not in capsule.keys()
        }
        # Add the parsed fields to the event capsule.
        capsule.update(parsed_fields_from_line)

        # Add a uuid for this event.
        capsule['uuid'] = capsule_uuid(capsule)

        return Event(capsule)

    def __repr__(self):
        return '<LogParser(\'%s\')>' % self.format
