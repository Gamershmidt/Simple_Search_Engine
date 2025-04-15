from cassandra.cluster import Cluster
import sys
import os
import subprocess
from collections import defaultdict

hdfs_input = sys.argv[1] if len(sys.argv) > 1 else "/tmp/index1"
title_path = sys.argv[2] if len(sys.argv) > 2 else "/tmp/doc_titles"

cluster = Cluster(['cassandra-server'])
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_engine
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
""")

session.set_keyspace('search_engine')

session.execute("""
    CREATE TABLE IF NOT EXISTS term_freq (
        term TEXT,
        doc_id TEXT,
        tf INT,
        PRIMARY KEY (term, doc_id)
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS doc_dreq (
        term TEXT PRIMARY KEY,
        df INT
    )
""")

session.execute("""
    CREATE TABLE IF NOT EXISTS doc_stats (
        doc_id TEXT PRIMARY KEY,
        length INT,
        title TEXT
    )
""")

cluster.shutdown()
print("Cassandra keyspace and tables created.")

try:
    result = subprocess.run(
        ["hdfs", "dfs", "-ls", hdfs_input],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
except Exception as e:
    print(f"HDFS input file does not exist: {hdfs_input}")
    sys.exit(1)

def load_doc_titles(hdfs_titles_path="/tmp/doc_titles", local_file="titles.txt"):
    titles = {}

    print(f"Downloading HDFS file: {hdfs_titles_path} to local file: {local_file}")
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-ls", hdfs_titles_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
    except Exception as e:
        print("Failed to get real titles")
        return titles
    if os.path.exists(local_file):
        os.remove(local_file)
    subprocess.run(["hdfs", "dfs", "-getmerge", hdfs_titles_path, local_file])
    with open(local_file, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("\t", 1)
            if len(parts) == 2:
                doc_id, title = parts
                titles[doc_id] = title
    return titles

def write_output_to_cassandra(hdfs_input_path, title_path):
    print("Connecting to Cassandra")
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')

    local_file = "index_output.txt"
    local_title_file = "titles.txt"

    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-ls", hdfs_input_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
    except Exception as e:
        print("Failed to get real titles")
        sys.exit(1)

    if os.path.exists(local_file):
        os.remove(local_file)
    
    subprocess.run(["hdfs", "dfs", "-getmerge", hdfs_input_path, local_file])

    doc_titles = load_doc_titles(title_path, local_title_file)
    vocabulary = defaultdict(set)
    doc_stats = defaultdict(int)

    with open(local_file, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 3:
                continue
            term, doc_id, tf_str = parts[:3]
            try:
                tf = int(tf_str)
            except ValueError:
                continue
            session.execute(
                "INSERT INTO term_index (term, doc_id, tf) VALUES (%s, %s, %s)",
                (term, doc_id, tf)
            )
            vocabulary[term].add(doc_id)
            doc_stats[doc_id] += tf

    print("Data written to term_index table.")

    for term, doc_ids in vocabulary.items():
        session.execute(
            "INSERT INTO vocabulary (term, df) VALUES (%s, %s)",
            (term, len(doc_ids) + 1)
        )
    print("Vocabulary table updated.")

    for doc_id, length in doc_stats.items():
        title = doc_titles.get(doc_id, "UNKNOWN")
        session.execute(
            "INSERT INTO doc_stats (doc_id, length, title) VALUES (%s, %s, %s)",
            (doc_id, length, title)
        )
    print("Document statistics table updated.")

    cluster.shutdown()
    print("Cassandra index insertion completed.")

write_output_to_cassandra(hdfs_input, title_path)
