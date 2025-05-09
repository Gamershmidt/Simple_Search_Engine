#!/bin/bash
echo "This script will include commands to search for documents given the query using Spark RDD"


source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)

export PYSPARK_PYTHON=./.venv/bin/python

QUERY="$1"

if [ -z "$QUERY" ]; then
  echo "Please provide a search query in quotes."
  echo "Usage: ./search.sh \"search terms here\""
  exit 1
fi

spark-submit \
  --master yarn \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
  --archives .venv.tar.gz#.venv \
  query.py "$1"
