#!/bin/bash

echo "Choose Dataset (small=0/big=1)"
read dataset

if [ $dataset == 0 ]; then
    wget https://files.grouplens.org/datasets/movielens/ml-latest-small.zip
    unzip ml-latest-small.zip
elif [ $dataset == 1 ]; then
    wget https://files.grouplens.org/datasets/movielens/ml-latest.zip
    unzip ml-latest.zip
else
    echo "Wrong Input"
fi