from pyspark.sql import SparkSession
import math
import sys

if len(sys.argv) < 2:
    print("Usage: python query.py <query text>")
    sys.exit(1)

query_text = sys.argv[1].lower()
query_terms = query_text.strip().split()

spark = SparkSession.builder \
    .appName("BM25 Search") \
    .config("spark.cassandra.connection.host", "cassandra-server") \
    .getOrCreate()

def load_table(table):
    return spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace="search_engine").load()

index_df = load_table("term_freq")
vocab_df = load_table("doc_freq")
doc_stats_df = load_table("doc_stats")

N = doc_stats_df.count()
avg_dl = doc_stats_df.selectExpr("avg(length)").collect()[0][0]

doc_stats_dict = doc_stats_df.rdd.map(lambda row: (row.doc_id, (row.length, row.title))).collectAsMap()
vocab_dict = vocab_df.filter(vocab_df.term.isin(query_terms)) \
    .rdd.map(lambda row: (row.term, row.df)).collectAsMap()

index_rdd = index_df.filter(index_df.term.isin(query_terms)).rdd

def BM25(tf, df, dl, avg_dl, N, k1=1.2, b=0.75):
    idf = math.log(N / df)
    num = tf * (k1 + 1)
    denom = tf + k1 * (1 - b + b * (dl / avg_dl))
    return idf * (num / denom)

def calc_bm25(index_rdd, vocab_dict, doc_stats_dict, N, avg_dl):
    def score(row):
        term = row.term
        doc_id = row.doc_id
        tf = row.tf
        if term in vocab_dict and doc_id in doc_stats_dict:
            df = vocab_dict[term]
            dl = doc_stats_dict[doc_id][0]
            score = BM25(tf, df, dl, avg_dl, N)
            return (doc_id, score)
        else:
            return (doc_id, 0.0)
    return index_rdd.map(score).reduceByKey(lambda a, b: a + b)

def get_top_results(score_rdd, doc_stats_dict, top_n=10):
    top_docs = score_rdd.takeOrdered(top_n, key=lambda x: -x[1])
    return [(doc_id, doc_stats_dict.get(doc_id, ("?", "UNKNOWN"))[1], score)
            for doc_id, score in top_docs]

score_rdd = calc_bm25(index_rdd, vocab_dict, doc_stats_dict, N, avg_dl)
top_results = get_top_results(score_rdd, doc_stats_dict, top_n=10)

print("\nTop 10 Results:")
for doc_id, title, score in top_results:
    print(f"{doc_id}\t{title}\tScore: {score:.4f}")
