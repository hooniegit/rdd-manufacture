#!/bin/bash

mkdir ../tlc
cd ../tlc

for year in {2022..2023}
do
  wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$year-01.parquet
  wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$year-02.parquet
  wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$year-03.parquet
  wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$year-04.parquet
  wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$year-05.parquet
  wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$year-06.parquet
  wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$year-07.parquet
  wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$year-08.parquet
  wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$year-09.parquet
  wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$year-10.parquet
  wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$year-11.parquet
  wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_$year-12.parquet
done
