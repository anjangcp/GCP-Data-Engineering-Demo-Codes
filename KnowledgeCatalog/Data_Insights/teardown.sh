#!/bin/zsh
# Removes the demo data and every data scan on it made for "AI in Knowledge Catalog" (ours and the
# ones the console creates for insights). Data scans for this US dataset live in us-central1. The APIs stay enabled.
: ${PROJECT_ID:?Set PROJECT_ID to your Google Cloud project ID}
P=$PROJECT_ID
for s in $(gcloud dataplex datascans list --project=$P --location=us-central1 \
             --format="value(name,data.resource)" 2>/dev/null | grep shop_raw | cut -f1); do
  gcloud dataplex datascans delete "$s" --quiet
done
bq rm -r -f --dataset "${P}:shop_raw"
