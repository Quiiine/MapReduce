from pyspark import SparkContext, SparkConf

# Initialize Spark context
conf = SparkConf().setAppName("WordCount")
sc = SparkContext(conf=conf)

# HDFS input file path (replace with your actual file path)
hdfs_input_path = "hdfs://localhost:9000/root/example.txt"

# Load the text file from HDFS
lines = sc.textFile(hdfs_input_path)

# Split lines into words and count them
word_counts = lines.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a + b)

# Collect the results and print the word counts
results = word_counts.collect()
for (word, count) in results:
    print(f"{word}: {count}")

# Stop Spark context
sc.stop()
