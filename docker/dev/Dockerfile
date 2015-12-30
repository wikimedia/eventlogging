FROM wikimedia/eventlogging

# Make sure you mount your local eventlogging codebase at
# /srv/eventlogging when running:
# docker run -i -t -v $(pwd):/srv/eventlogging wikimedia/eventlogging:dev

# Change PYTHONPATH to use the development codebase, and
# put development executables ahead of other PATHS
ENV PYTHONPATH="/srv/eventlogging"
ENV PATH="/srv/eventlogging/bin:$PATH"
