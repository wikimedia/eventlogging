FROM wikimedia/eventlogging

RUN git clone https://gerrit.wikimedia.org/r/p/mediawiki/event-schemas /etc/event-schemas

ENTRYPOINT [                                        \
  "/usr/bin/python",                                \
  "-OO",                                            \
  "/usr/src/eventlogging/bin/eventlogging-service", \
  "--topic-config",                                 \
  "/etc/event-schemas/config/eventbus-topics.yaml", \
  "--schemas-path",                                 \
  "/etc/event-schemas/jsonschema"                   \
]
CMD ["stdout://"]
