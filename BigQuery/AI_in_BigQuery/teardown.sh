#!/bin/bash
# Deletes the demo dataset and every table in it. This cannot be undone.
: ${PROJECT_ID:?Set PROJECT_ID to your Google Cloud project ID}
bq rm -r -f --dataset ${PROJECT_ID}:shop && echo "Deleted dataset shop."
