# AI in Pub/Sub: add Gemini to a data stream with no code

Demo code for the YouTube video "AI in Pub/Sub". Customer reviews published to a Pub/Sub topic are classified by Gemini on the way through and written to BigQuery already labelled, using Pub/Sub's AI Inference single message transform (SMT).

| File | What it is |
|---|---|
| `ai-smt.yaml` | The simplest case: one AI Inference transform |
| `classify-chain.yaml` | Three transforms in order: a JavaScript function that builds the Gemini request, the AI Inference transform, and a JavaScript function that keeps only the review and its topic |
| `setup.sh` | Creates the topic, a BigQuery subscription with the three transforms, a dead-letter topic, the destination table and the IAM roles |
| `sample-messages.txt` | Five reviews to publish, plus one bad message for the dead-letter demo |
| `teardown.sh` | Removes the topics, subscriptions and table |

Replace `PROJECT_ID` in the YAML files with your project ID if you use them directly; `setup.sh` does this for you.

## Release status (checked 14 October 2026)

| Feature | Status |
|---|---|
| AI Inference SMT | Generally available since 6 April 2026 |
| JavaScript UDF SMT | No Preview label in the documentation |
| BigQuery subscriptions | Generally available |

## Before you run it

1. Enable the Pub/Sub and Gemini Enterprise Agent Platform APIs.
2. Pub/Sub calls the model as its own service account (`service-PROJECT_NUMBER@gcp-sa-pubsub.iam.gserviceaccount.com`). It needs **Agent Platform User**; for the BigQuery subscription, **BigQuery Data Editor** on the table; for the dead-letter topic, publisher on that topic and subscriber on the subscription. `setup.sh` grants these.
3. Run `PROJECT_ID=your-project ./setup.sh`, then publish the lines from `sample-messages.txt`.

## Things we observed in our run (not stated in the documentation)

- The newer Gemini models answered only at the `global` location in our project, so the endpoint uses `locations/global`.
- A BigQuery subscription with the `global` endpoint and a table in the US multi-region worked.
- The model output was about 2 KB for a two-word answer, which is why the last JavaScript transform keeps only the answer.

## Cost

$60 per TiB of data through the AI Inference transform (measured on the enriched message) plus the model's own charges; $40 per TiB for JavaScript transforms. See the [Pub/Sub pricing page](https://cloud.google.com/pubsub/pricing).

Reference: [AI Inference SMT](https://docs.cloud.google.com/pubsub/docs/smts/ai-inference-smt)
