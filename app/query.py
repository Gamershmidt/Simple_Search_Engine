from pyspark.sql import SparkSession
import math
import sys


def BM25(tf, df, dl, avg_dl, N, k1=1.2, b=0.75):
    idf = math.log(N / df)
    num = tf * (k1 + 1)
    denom = tf + k1 * (1 - b + b * (dl / avg_dl))
    return idf * (num / denom)


def init_spark(app_name="BM25 Search", cassandra_host="cassandra-server"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.cassandra.connection.host", cassandra_host) \
        .getOrCreate()


def load_tables(spark, keyspace="search_engine"):
    def load_table(table):
        return spark.read.format("org.apache.spark.sql.cassandra") \
            .options(table=table, keyspace=keyspace).load()

    index_df = load_table("term_index")
    vocab_df = load_table("vocabulary")
    doc_stats_df = load_table("doc_stats")
    return index_df, vocab_df, doc_stats_df


def calculate_bm25_scores(index_rdd, vocab_dict, doc_stats_dict, N, avg_dl):
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


def main():
    if len(sys.argv) < 2:
        print("Usage: python query.py <query text>")
        sys.exit(1)

    query_text = sys.argv[1].lower()
    query_terms = query_text.strip().split()
    print(f"Searching for: {query_text}")

    spark = init_spark()
    index_df, vocab_df, doc_stats_df = load_tables(spark)

    N = doc_stats_df.count()
    avg_dl = doc_stats_df.selectExpr("avg(length)").collect()[0][0]

    # Broadcast doc stats
    doc_stats = doc_stats_df.rdd.map(lambda row: (row.doc_id, (row.length, row.title))).collectAsMap()
    vocab = vocab_df.filter(vocab_df.term.isin(query_terms)) \
        .rdd.map(lambda row: (row.term, row.df)).collectAsMap()

    # Filter term_index for relevant terms
    index_rdd = index_df.filter(index_df.term.isin(query_terms)).rdd

    # Score and rank
    score_rdd = calculate_bm25_scores(index_rdd, vocab, doc_stats, N, avg_dl)
    top_results = get_top_results(score_rdd, doc_stats, top_n=10)

    print("\nTop 10 Results:")
    for doc_id, title, score in top_results:
        print(f"{doc_id}\t{title}\tScore: {score:.4f}")


if __name__ == "__main__":
    main()
