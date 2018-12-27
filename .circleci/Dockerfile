FROM ubuntu:16.04

RUN apt-get update && apt-get install -y curl
RUN curl -s https://packagecloud.io/install/repositories/basho/riak/script.deb.sh | bash
RUN apt-get update && apt-get install -y riak=2.2.0-1
RUN sed -i 's/storage_backend.*/storage_backend = leveldb/' /etc/riak/riak.conf && \
    service riak restart && \
    riak version && \
    riak-admin bucket-type create maps '{"props":{"datatype":"map","backend":"leveldb"}}' && \
    riak-admin bucket-type activate maps && \
    riak-admin bucket-type create tests '{"props":{"backend":"leveldb"}}' && \
    riak-admin bucket-type activate tests && \
    riak-admin bucket-type create hlls '{"props":{"datatype":"hll","backend":"leveldb"}}' && \
    riak-admin bucket-type activate hlls

