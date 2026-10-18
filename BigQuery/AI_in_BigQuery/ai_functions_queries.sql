-- Queries shown in "AI in BigQuery: Gemini With Nothing but SQL".
-- All of these were run on 11 October 2026 in our demo project, dataset shop (US multi-region),
-- against 400 made-up reviews created by demo/setup.sh. Results are noted under each query.
-- AI results are not identical from run to run, so expect small differences.
-- Prerequisites: the Agent Platform API enabled, and the Agent Platform User role on your account.

-- shop.reviews(review_id INT64, product STRING, review_date DATE, review_text STRING)

-- Test on a few rows before running on the whole table.
SELECT review_id, AI.IF(('This review is a complaint: ', review_text)) AS is_complaint, review_text
FROM shop.reviews
LIMIT 8;

-- 1. Filter with AI.IF: keep only the complaints, and save them.
CREATE OR REPLACE TABLE shop.complaints AS
SELECT *
FROM shop.reviews
WHERE AI.IF(('This review is a complaint: ', review_text));
-- Result: 400 reviews -> 137 complaints.

-- 2. Categorise with AI.CLASSIFY: one label per row, then count.
SELECT
  AI.CLASSIFY(
    review_text,
    ['Delivery', 'Product quality', 'Price', 'Customer service']) AS topic,
  COUNT(*) AS complaints
FROM shop.complaints
GROUP BY topic
ORDER BY complaints DESC;
-- Result: Product quality 68, Delivery 33, Customer service 28, Price 8.

-- 3. Rank with AI.SCORE: the five most urgent.
SELECT
  review_text,
  AI.SCORE(('On a scale from 1 to 10, rate how urgent this complaint is: ', review_text)) AS urgency
FROM shop.complaints
ORDER BY urgency DESC
LIMIT 5;
-- Result: five reviews scored 10.0, all electrical or burning-smell faults.

-- 4a. Extract with AI.GENERATE: free text in, typed columns out.
SELECT
  review_id,
  AI.GENERATE(
    review_text,
    output_schema => 'problem STRING, days_owned INT64, wants_refund BOOL').* EXCEPT (full_response, status)
FROM shop.complaints
ORDER BY review_id;
-- Result: one row per complaint with problem, days_owned and wants_refund.
-- Worth knowing: a late-delivery review that never mentions ownership came back with days_owned = 0,
-- and its wants_refund value differed between two runs. Leave out the EXCEPT clause to see the status column.

-- 4b. AI.GENERATE without a schema: a one-sentence summary.
SELECT
  review_id,
  AI.GENERATE(CONCAT('Summarize in one sentence: ', review_text)).result AS summary
FROM shop.complaints
ORDER BY review_id
LIMIT 3;

-- 5. Forecast with AI.FORECAST: the next 30 days of daily complaints.
CREATE OR REPLACE TABLE shop.daily_complaints AS
SELECT day, COUNTIF(c.review_id IS NOT NULL) AS complaints
FROM UNNEST(GENERATE_DATE_ARRAY(
       (SELECT MIN(review_date) FROM shop.reviews),
       (SELECT MAX(review_date) FROM shop.reviews))) AS day
LEFT JOIN shop.complaints AS c ON c.review_date = day
GROUP BY day;

SELECT *
FROM AI.FORECAST(
  TABLE shop.daily_complaints,
  data_col => 'complaints',
  timestamp_col => 'day',
  horizon => 30)
ORDER BY forecast_timestamp;
-- Result: about 2 complaints a day forecast (average 2.07), against 0.73 a day in the first 30 days
-- of the data and 2.37 a day in the last 30. Lower bounds can be negative even though counts cannot.
