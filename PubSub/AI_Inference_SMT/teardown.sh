#!/bin/bash
# Remove the demo resources created by setup.sh. The role grants on the Pub/Sub service agent are left in place.
: ${PROJECT_ID:?Set PROJECT_ID to your Google Cloud project ID}
P=$PROJECT_ID
gcloud pubsub subscriptions delete reviews-to-bigquery reviews-dead-letter-sub --project=$P -q
gcloud pubsub topics delete reviews reviews-dead-letter --project=$P -q
bq rm -f -t "${P}:shop.labelled_reviews"
