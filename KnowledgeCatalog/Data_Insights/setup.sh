#!/bin/zsh
# Demo data for "AI in Knowledge Catalog": the shop's raw source tables,
# with short cryptic column names and no descriptions, the way data often lands from an old system.
# The review text comes from shop.reviews, created by ../../BigQuery/AI_in_BigQuery/setup.sh (run that
# first); everything else is generated here, the same on every run.
# Usage:  PROJECT_ID=your-project ./setup.sh      Remove everything with ./teardown.sh
set -e
: ${PROJECT_ID:?Set PROJECT_ID to your Google Cloud project ID}
P=$PROJECT_ID

# APIs listed on the "Generate table insights" page: Dataplex, BigQuery, Gemini for Google Cloud.
gcloud services enable dataplex.googleapis.com bigquery.googleapis.com cloudaicompanion.googleapis.com --project=$P

bq --location=US mk --dataset "${P}:shop_raw" 2>/dev/null || true

bq query --use_legacy_sql=false --location=US --project_id=$P <<'SQL'
-- Products: the five reviewed products plus seven more.
CREATE OR REPLACE TABLE shop_raw.prd AS
SELECT prd_id, prd_nm, cat_cd, unt_prc, actv_flg FROM UNNEST([
  STRUCT(101 AS prd_id, 'Air fryer' AS prd_nm, 'KAP' AS cat_cd, 89.00 AS unt_prc, 'Y' AS actv_flg),
  (102, 'Blender', 'KAP', 59.00, 'Y'), (103, 'Coffee machine', 'KAP', 199.00, 'Y'),
  (104, 'Electric kettle', 'KAP', 39.00, 'Y'), (105, 'Toaster', 'KAP', 45.00, 'Y'),
  (106, 'Chef knife set', 'CKW', 79.00, 'Y'), (107, 'Frying pan 28 cm', 'CKW', 35.00, 'Y'),
  (108, 'Stock pot', 'CKW', 49.00, 'N'), (109, 'Glass storage jars', 'STR', 25.00, 'Y'),
  (110, 'Bread bin', 'STR', 29.00, 'Y'), (111, 'Kitchen scale', 'ACC', 19.00, 'Y'),
  (112, 'Oven gloves', 'ACC', 12.00, 'Y')]);

-- Customers: 300, with a segment code (R retail, W wholesale, V VIP).
-- RND(x, k) below is a repeatable pseudo-random number in [0, k): the same data on every run.
CREATE TEMP FUNCTION RND(x STRING, k INT64) AS (MOD(ABS(FARM_FINGERPRINT(x)), k));

CREATE OR REPLACE TABLE shop_raw.cst AS
SELECT 5000 + n AS cst_id,
  ['Asha','Ravi','Meera','John','Sara','Imran','Li','Priya','Tom','Fatima','Arjun','Emma'][OFFSET(RND(CONCAT('f', n), 12))] AS f_nm,
  ['Rao','Iyer','Khan','Smith','Patel','Chen','Das','Brown','Nair','Singh'][OFFSET(RND(CONCAT('l', n), 10))] AS l_nm,
  CONCAT('user', CAST(n AS STRING), '@example.com') AS eml,
  ['Mumbai','Delhi','Bengaluru','Chennai','Pune','Hyderabad','Kolkata'][OFFSET(RND(CONCAT('c', n), 7))] AS cty,
  DATE_SUB(DATE '2026-09-30', INTERVAL RND(CONCAT('d', n), 900) DAY) AS crt_dt,
  CASE WHEN RND(CONCAT('s', n), 100) < 5 THEN 'V' WHEN RND(CONCAT('s', n), 100) < 20 THEN 'W' ELSE 'R' END AS seg_cd
FROM UNNEST(GENERATE_ARRAY(1, 300)) AS n;

-- Orders: 1,200 between April and September 2026. Status D delivered, S shipped, C cancelled, R returned.
-- A few customers order often and some return a lot, as in a real shop.
CREATE OR REPLACE TABLE shop_raw.ord AS
SELECT 90000 + n AS ord_id,
  5001 + IF(RND(CONCAT('o', n), 4) = 0, RND(CONCAT('h', n), 30), RND(CONCAT('k', n), 300)) AS cst_id,
  TIMESTAMP_ADD(TIMESTAMP '2026-04-01 08:00:00', INTERVAL RND(CONCAT('t', n), 183 * 24 * 60) MINUTE) AS ord_ts,
  CASE WHEN RND(CONCAT('x', n), 100) < 6 THEN 'C' WHEN RND(CONCAT('x', n), 100) < 15 THEN 'R'
       WHEN RND(CONCAT('x', n), 100) < 30 THEN 'S' ELSE 'D' END AS ord_sts,
  ['Mumbai','Delhi','Bengaluru','Chennai','Pune','Hyderabad','Kolkata'][OFFSET(RND(CONCAT('y', n), 7))] AS shp_cty
FROM UNNEST(GENERATE_ARRAY(1, 1200)) AS n;

-- Order lines: one to three per order.
CREATE OR REPLACE TABLE shop_raw.ord_ln AS
SELECT o.ord_id, ln AS ln_no, p.prd_id, 1 + RND(CONCAT('q', o.ord_id, '-', ln), 3) AS qty, p.unt_prc,
  [0, 0, 0, 5, 10, 15][OFFSET(RND(CONCAT('p', o.ord_id, '-', ln), 6))] AS dsc_pct
FROM shop_raw.ord AS o, UNNEST(GENERATE_ARRAY(1, 1 + RND(CONCAT('n', o.ord_id), 3))) AS ln
JOIN shop_raw.prd AS p ON p.prd_id = 101 + RND(CONCAT('r', o.ord_id, '-', ln), 12);

-- Reviews: the 400 real review texts from video 2, linked to products and customers.
CREATE OR REPLACE TABLE shop_raw.rvw AS
SELECT r.review_id AS rv_id, p.prd_id, 5001 + RND(CONCAT('v', r.review_id), 300) AS cst_id,
  r.review_date AS rv_dt, r.review_text AS rv_txt
FROM shop.reviews AS r JOIN shop_raw.prd AS p ON p.prd_nm = r.product;
SQL

# Profile scans run as the Knowledge Catalog (Dataplex) service agent: it needs BigQuery Job User on the
# project and read access to the data ("Use data profiling" page). Granting at dataset level keeps the
# access when the tables are recreated. The service agent is created first if it doesn't exist yet.
N=$(gcloud projects describe $P --format="value(projectNumber)")
SA="service-${N}@gcp-sa-dataplex.iam.gserviceaccount.com"
curl -s -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application/json" \
  "https://serviceusage.googleapis.com/v1beta1/projects/${N}/services/dataplex.googleapis.com:generateServiceIdentity" -d '{}' >/dev/null
gcloud projects add-iam-policy-binding $P --member="serviceAccount:${SA}" --role=roles/bigquery.jobUser --condition=None --quiet >/dev/null
bq query --use_legacy_sql=false --location=US --project_id=$P \
  "GRANT \`roles/bigquery.dataViewer\` ON SCHEMA shop_raw TO 'serviceAccount:${SA}'" >/dev/null

bq ls "${P}:shop_raw"

# Link the orders table to the profile scan made in the console (create it in the console with the ID shop-raw-ord-profile), as the
# "Generate table insights" page describes; in our test the scan's publish option did not add these labels.
bq update --set_label dataplex-dp-published-scan:shop-raw-ord-profile --set_label dataplex-dp-published-project:${P} \
  --set_label dataplex-dp-published-location:us-central1 "${P}:shop_raw.ord" >/dev/null
