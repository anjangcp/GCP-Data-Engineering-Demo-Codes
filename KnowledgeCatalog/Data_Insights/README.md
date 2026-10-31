# AI in Knowledge Catalog: let Gemini document your data

Demo code for the YouTube video "AI in Knowledge Catalog". It builds a small shop dataset, `shop_raw`, whose five tables have short, unclear column names (`cst_id`, `ord_sts`, `shp_cty`, `seg_cd`) and no descriptions. In the video, a data profile scan finds the real values, Gemini's data insights write a first draft of the documentation, a person reviews and saves it, and Knowledge Catalog search can then find the tables.

| File | What it is |
|---|---|
| `setup.sh` | Creates `shop_raw` (customers, orders, order lines, products, reviews), grants the Knowledge Catalog service agent read access, and links the orders table to its profile scan |
| `teardown.sh` | Deletes the data scans on `shop_raw` and the dataset |

## Before you run it

1. Run `../../BigQuery/AI_in_BigQuery/setup.sh` first: the reviews table here reuses its 400 review texts.
2. Run `PROJECT_ID=your-project ./setup.sh`. It enables the Dataplex, BigQuery and Gemini for Google Cloud APIs.
3. Set up Gemini in BigQuery on the project (see the Google Cloud documentation below).
4. In the console, follow the steps in the video: profile scan on `ord` (ID `shop-raw-ord-profile`, location `us-central1`, publish results on), then table insights, review and save, dataset insights, and search.

Expected numbers: 1,200 orders; 817 delivered (`D`), 888 if `C` is wrongly counted as completed.

## Release status (checked 31 October 2026)

| Feature | Status |
|---|---|
| Data insights (table and dataset insights) | Generally available |
| Natural language search in Knowledge Catalog | Generally available |
| Publish generated insights to Knowledge Catalog | Preview |

## Cost

Data insights is charged per token ($3 per million input, $20 per million output) from 27 October 2026, per Google's Data Cloud Agent pricing page. Profile scans are billed as Knowledge Catalog premium processing. Run `teardown.sh` when you are done.

## Things we observed in our run (not stated in the documentation)

- Data scans for a dataset in the `US` multi-region had to be created in `us-central1`; the location `us` was refused.
- Publishing the profile scan's results did not add the `dataplex-dp-published-*` labels to the table, so `setup.sh` adds them, as the "Generate table insights" page describes.
- Gemini described `ord_sts` as "such as D, S, or R": it left out `C` (cancelled). Always review generated descriptions of codes against the profile.

## Documentation

- Data insights: https://docs.cloud.google.com/bigquery/docs/data-insights
- Generate table insights: https://docs.cloud.google.com/bigquery/docs/generate-table-insights
- Generate dataset insights: https://docs.cloud.google.com/bigquery/docs/generate-dataset-insights
- Data profiling: https://docs.cloud.google.com/knowledge-catalog/docs/use-data-profiling
- Search: https://docs.cloud.google.com/knowledge-catalog/docs/about-search
