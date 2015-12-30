FROM wikimedia/eventlogging

COPY topics.yaml /etc/topics.yaml

ENTRYPOINT [                                            \
  "/usr/bin/python",                                    \
  "-OO",                                                \
  "/usr/src/eventlogging/bin/eventlogging-service",     \
  "--topic-config",                                     \
  "/etc/topics.yaml",                                   \
  "--schemas-path",                                     \
  "/usr/src/eventlogging/config/schemas/jsonschema"     \
]
CMD ["stdout://"]
