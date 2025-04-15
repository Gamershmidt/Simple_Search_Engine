import os
import sys
import shutil
import ntpath
from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from tqdm import tqdm
import shutil

print("\n\n\n\n\n\ndata preparation!\n\n\n\n\n")
output_dir = "data"
os.makedirs(output_dir, exist_ok=True)

spark = SparkSession.builder \
    .appName("data preparation") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.hadoop.parquet.enable.dictionary", "true") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .master("local") \
    .getOrCreate()

print("\n\n\n\n\n\nread parquet file!\n\n\n\n\n")
df = spark.read.parquet("/a.parquet")

n = 10
df_sample = df.select("id", "title", "text") \
    .filter(col("text").isNotNull()) \
    .filter(col("text") != "") \
    .limit(n)



rows = list(df_sample.toLocalIterator())


print("\n\n\n\n\n\nwriting files!\n\n\n\n\n")
def create_doc(row):
    title = sanitize_filename(row["title"]).replace(" ", "_")
    filename = os.path.join(output_dir, f"{row['id']}_{title}.txt")
    with open(filename, "w", encoding="utf-8") as f:
        f.write(row["text"])

for row in tqdm(rows, desc="Writing", file=sys.stdout, dynamic_ncols=True):
    create_doc(row)

print("\n\n\n\n\n\nrdd!\n\n\n\n\n")

rdd = spark.sparkContext.wholeTextFiles("file:///app/data")

def format_for_index(path_text_tuple):
    path, text = path_text_tuple
    filename = ntpath.basename(path)
    base = filename[:-4]
    try:
        doc_id, title = base.split("_", 1)
        return f"{doc_id}\t{title}\t{text.strip()}"
    except Exception:
        return None

index_rdd = rdd.map(format_for_index).filter(lambda x: x is not None)


hadoop_path = "/index/data"
hadoop = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
path = spark._jvm.org.apache.hadoop.fs.Path(hadoop_path)
if hadoop.exists(path):
    hadoop.delete(path, True)

index_rdd.coalesce(1).saveAsTextFile(hadoop_path)


spark.stop()