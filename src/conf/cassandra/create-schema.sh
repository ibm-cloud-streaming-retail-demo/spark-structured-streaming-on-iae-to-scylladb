#!/bin/bash

# create Cassandra schema
cqlsh -f /schema.cql;

# confirm schema
cqlsh -e "DESCRIBE SCHEMA;"