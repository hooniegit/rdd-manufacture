import os
from time import time

# record start time
start_time = time()

now_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(now_dir, "../../../../../datas/ml-latest/ratings.csv")

from pyspark.sql import SparkSession

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


# change rdd to dataframe
column = ['userId','movieId','rating','date','time']
df = rdd.toDF(column)
df.show()

# stop spark session
spark.stop()

# record end time & print result
end_time = time()
print(f"spent : {end_time - start_time}")