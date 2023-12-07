import os
from time import time

# record start time
start_time = time()

now_dir = os.path.dirname(os.path.abspath(__file__))
scores_dir = os.path.join(now_dir, "../../../../../datas/ml-latest/genome-scores.csv")
tags_dir = os.path.join(now_dir, "../../../../../datas/ml-latest/genome-tags.csv")
movies_dir = os.path.join(now_dir, "../../../../../datas/ml-latest/movies.csv")

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# build spark session
conf = SparkConf().setAppName("csv_count_genres")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# manufacture rdd
rdd_scores = sc.textFile(scores_dir).map(lambda line: line.split(',')) \
                                    .map(lambda row: (row[1], row[0], row[2]))
                                    
# df_result = rdd_scores.toDF()
# df_result.show()                                    
                    
rdd_tags = sc.textFile(tags_dir).map(lambda line: line.split(',')) \
                                .map(lambda row: (row[0], row[1]))
# df_result = rdd_tags.toDF()
# df_result.show()

rdd_genome = rdd_scores.join(rdd_tags)
                       
# transfer rdd to dataframe
df_result = rdd_genome.toDF()
df_result.show()

# stop spark session
spark.stop()

# record end time & print result
end_time = time()
print(f"spent : {end_time - start_time}")