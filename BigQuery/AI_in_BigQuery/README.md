# AI in BigQuery: Gemini with nothing but SQL

Demo code for the YouTube video "AI in BigQuery: Use Gemini With Nothing but SQL". One table of made-up customer reviews, five tasks, five BigQuery AI functions.

| File | What it is |
|---|---|
| `make_reviews.py` | Generates 400 made-up reviews (`reviews.jsonl`). Fixed seed, so the data is the same every time |
| `setup.sh` | Creates dataset `shop` in the US multi-region and loads the reviews |
| `ai_functions_queries.sql` | The queries from the video, in order, with the results we got noted under each |
| `teardown.sh` | Deletes the dataset |

## Functions used

| Function | Task | Release status (checked 11 October 2026) |
|---|---|---|
| `AI.IF` | Filter rows with a yes-or-no question | Generally available |
| `AI.CLASSIFY` | Label rows with categories you choose | Generally available |
| `AI.SCORE` | Rank rows by a score from a prompt | Generally available |
| `AI.GENERATE` | Extract typed fields, summarise, translate | Generally available |
| `AI.FORECAST` | Forecast a time series with no training | Generally available (default model TimesFM 2.5) |

## Before you run it

1. Enable the Gemini Enterprise Agent Platform API (formerly Vertex AI API) on your project.
2. Your account needs the **Agent Platform User** role (`roles/aiplatform.user`).
3. Run `PROJECT_ID=your-project ./setup.sh`, then the statements in `ai_functions_queries.sql`.

Every row sent to an AI function is a paid model call. The demo table is small (400 rows), but test on a few rows with `LIMIT` before running these on your own tables. AI results are not identical from run to run.

Reference: [Introduction to AI in BigQuery](https://docs.cloud.google.com/bigquery/docs/ai-introduction)
