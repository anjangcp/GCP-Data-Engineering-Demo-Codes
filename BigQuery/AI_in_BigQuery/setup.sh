#!/bin/bash
# Creates the demo dataset for "AI in BigQuery" and loads 400 made-up customer reviews.
# Usage:  PROJECT_ID=your-project ./setup.sh      Remove everything with ./teardown.sh
# The dataset is created in the US multi-region so the AI functions need no endpoint argument.
set -e
: ${PROJECT_ID:?Set PROJECT_ID to your Google Cloud project ID}
python3 make_reviews.py                     # writes reviews.jsonl
bq --location=US mk --dataset --force --description "Made-up shop data for the AI in BigQuery demo" ${PROJECT_ID}:shop
bq load --source_format=NEWLINE_DELIMITED_JSON --replace ${PROJECT_ID}:shop.reviews reviews.jsonl \
  review_id:INTEGER,product:STRING,review_date:DATE,review_text:STRING
echo "Loaded shop.reviews. Now run the statements in ai_functions_queries.sql in order."
