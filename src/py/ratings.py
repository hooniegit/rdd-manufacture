import os

now_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(now_dir, "../../../../datas/ml-latest/ratings.csv")

from pyspark.sql import SparkSession
from pyspark.sql.functions import split

# build spark session
spark = SparkSession.builder.appName("csv_change_timestamp").getOrCreate()

# filter func: define convertness & return bool
def is_int_convertible(value):
    try:
        int(value)
        return True
    except ValueError:
        return False

# map func: return dataset
def convert_timestamp(row):
    from datetime import datetime
    
    timestamp = int(row[3])
    dt_object = datetime.fromtimestamp(timestamp)
    formatted_date = dt_object.strftime('%Y-%m-%d')
    formatted_time = dt_object.strftime('%H:%M:%S')
    return (row[0], row[1], row[2], formatted_date, formatted_time)

# create & manufacture rdd
rdd = spark.sparkContext.textFile(data_dir).map(lambda line: line.split(',')) \
                                           .filter(lambda x: all(item != "" for item in x)) \
                                           .filter(lambda x: len(x) == 4) \
                                           .filter(lambda x: is_int_convertible(x[3])) \
                                           .map(convert_timestamp)
             
# collect rdd
# rdd.collect()
      
# manufacture rdd     
rdd_after = rdd.map(lambda row: (row[2], 1)) \
               .reduceByKey(lambda x, y: x + y) \
               .sortBy(lambda row: row[1], ascending=False)

# change rdd to dataframe
column = ['rating','count']
df = rdd_after.toDF(column)
df.show()

# stop spark session
spark.stop()