#!/bin/bash

mkdir -p data
cd data/
wget https://latana-data-eng-challenge.s3.eu-central-1.amazonaws.com/allposts.csv
cd ../
python run.py
rm -rf data	
