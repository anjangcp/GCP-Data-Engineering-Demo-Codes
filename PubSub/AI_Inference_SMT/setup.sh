#!/bin/bash
# Demo resources for "AI in Pub/Sub". Every step follows the Pub/Sub documentation.
# Usage:  PROJECT_ID=your-project ./setup.sh      Remove everything with ./teardown.sh
# Needs the BigQuery dataset "shop" from ../../BigQuery/AI_in_BigQuery (or create it first).
set -e
: ${PROJECT_ID:?Set PROJECT_ID to your Google Cloud project ID}
P=$PROJECT_ID
sed "s#PROJECT_ID#$P#" classify-chain.yaml > /tmp/classify-chain.yaml
N=$(gcloud projects describe $P --format="value(projectNumber)")
SA="service-${N}@gcp-sa-pubsub.iam.gserviceaccount.com"     # the Pub/Sub service agent

gcloud services enable pubsub.googleapis.com aiplatform.googleapis.com --project=$P
# The AI transform calls the model as the Pub/Sub service agent (AI Inference SMT page).
gcloud projects add-iam-policy-binding $P --member="serviceAccount:${SA}" --role=roles/aiplatform.user --condition=None >/dev/null

# Demo C destination; the service agent needs BigQuery Data Editor (Create BigQuery subscriptions page).
bq query --use_legacy_sql=false --location=US \
  "CREATE TABLE IF NOT EXISTS shop.labelled_reviews (review_id INT64, review_text STRING, topic STRING)"
bq add-iam-policy-binding --member="serviceAccount:${SA}" --role=roles/bigquery.dataEditor "${P}:shop.labelled_reviews" >/dev/null

gcloud pubsub topics create reviews --project=$P
gcloud pubsub topics create reviews-dead-letter --project=$P
gcloud pubsub subscriptions create reviews-dead-letter-sub --project=$P --topic=reviews-dead-letter
# Dead-letter forwarding needs publisher on the dead-letter topic and subscriber on the source subscription.
gcloud pubsub topics add-iam-policy-binding reviews-dead-letter --project=$P \
  --member="serviceAccount:${SA}" --role=roles/pubsub.publisher >/dev/null

sleep 30   # let the role grants take effect; creation fails with a permission error otherwise
gcloud pubsub subscriptions create reviews-to-bigquery --project=$P --topic=reviews \
  --bigquery-table="${P}.shop.labelled_reviews" --use-table-schema \
  --message-transforms-file=/tmp/classify-chain.yaml \
  --dead-letter-topic=reviews-dead-letter --max-delivery-attempts=5
gcloud pubsub subscriptions add-iam-policy-binding reviews-to-bigquery --project=$P \
  --member="serviceAccount:${SA}" --role=roles/pubsub.subscriber >/dev/null
echo "Ready. Publish with: gcloud pubsub topics publish reviews --message='{\"review_id\": 1, \"review_text\": \"...\"}'"
