#!/bin/bash

INPUT=${1:-/index/data}
OUTPUT_PATH="/tmp/index1"
OUTPUT_TITLE_PATH="/tmp/doc_titles"
HADOOP_STREAMING_JAR=$(find /usr/local/hadoop/share/hadoop/tools/lib/ -name "hadoop-streaming*.jar" | head -n 1)

if [[ "$INPUT" == hdfs://* || "$INPUT" == /* ]]; then
    echo "Using HDFS input: $INPUT"
else
    echo "Detected local file. Copying $INPUT to HDFS..."
    BASENAME=$(basename "$INPUT")
    HDFS_INPUT="/tmp/input_$BASENAME"
    hdfs dfs -rm -f "$HDFS_INPUT"
    hdfs dfs -put "$INPUT" "$HDFS_INPUT"
    INPUT="$HDFS_INPUT"
fi

echo "Cleaning previous output..."
hdfs dfs -rm -r -f "$OUTPUT_PATH"
hdfs dfs -rm -r -f "$OUTPUT_TITLE_PATH"

echo "Running Hadoop streaming job..."

hadoop jar "$HADOOP_STREAMING_JAR" \
  -input "$INPUT" \
  -output "$OUTPUT_PATH" \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -file mapreduce/mapper1.py \
  -file mapreduce/reducer1.py

hadoop jar $HADOOP_STREAMING_JAR \
    -input "$INPUT" \
    -output "$OUTPUT_TITLE_PATH" \
    -mapper "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -file mapreduce/mapper2.py \
    -file mapreduce/reducer2.py


echo "Preview output:"
hdfs dfs -cat "$OUTPUT_PATH/part-00000" | head -n 10

echo "Populating Cassandra index..."
python3 app.py "$OUTPUT_PATH" "$OUTPUT_TITLE_PATH"

echo "Indexing complete."
