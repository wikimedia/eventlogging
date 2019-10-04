FROM debian:stretch
MAINTAINER Andrew Otto <otto@wikimedia.org>

WORKDIR /tmp
RUN apt-get update && \
    apt-get install -y gnupg wget

COPY debian-backports.list /etc/apt/sources.list.d/debian-backports.list
COPY wikimedia.list /etc/apt/sources.list.d/wikimedia.list
COPY wikimedia.key /tmp/wikimedia.key
RUN apt-key add wikimedia.key > /dev/null
RUN apt-get update

RUN apt-get install -y --no-install-recommends \
  git                   \
  ca-certificates       \
  python3-dateutil       \
  python3-jsonschema     \
  python3-pygments       \
  python3-mysql.connector \
  python3-sqlalchemy     \
  python3-kafka          \
  python3-zmq            \
  python3-statsd         \
  python3-sprockets-mixins-statsd \
  python3-yaml           \
  python3-psutil         \
  python3-ua-parser

RUN mkdir -p /usr/src
WORKDIR /usr/src
RUN git clone https://gerrit.wikimedia.org/r/p/eventlogging
WORKDIR /usr/src/eventlogging
RUN git submodule update --init

ENV PYTHONPATH="/usr/src/eventlogging"
ENV PATH="$PATH:/usr/src/eventlogging/bin"

# Default to InnoDB in case MySQL writer is used.
ENV EVENTLOGGING_MYSQL_ENGINE=InnoDB

ENTRYPOINT ["/bin/bash"]
