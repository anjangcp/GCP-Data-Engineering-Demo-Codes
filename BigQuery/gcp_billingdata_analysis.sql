
/*

Author : @ Anjan GCP Data Engineering

SQLs to analyze Billing data based on different dimensions

*/

/************** Billing main columns and plain data  *****************/
SELECT
  invoice.month,
  service.description as service,
  usage_start_time,
  usage_end_time,
  project.name,
  location.region,
  cost,
  currency,
  usage.amount,
  usage.unit,
  (select SUM(c.amount)
  from UNNEST(credits) c) as credits_amount,
  (select STRING_AGG(c.full_name)
  from UNNEST(credits) c) as crdit_full_name
FROM
  `gcp-data-eng-374308.analysis.gcp_billing_export_v1_01CBD1_B38C45_E20EEB`;

/************** Total uasge cost per month  without Credits *****************/

SELECT
  invoice.month,
  SUM(cost) AS total,
  SUM(CAST(cost AS NUMERIC)) AS total_exact
FROM `gcp-data-eng-374308.analysis.gcp_billing_export_v1_01CBD1_B38C45_E20EEB`
GROUP BY 1
ORDER BY 1 ASC;

/************** Total uasge cost per month  with Credits  *****************/

SELECT
  invoice.month,
  SUM(cost)
    + SUM(IFNULL((SELECT SUM(c.amount)
                  FROM UNNEST(credits) c), 0))
    AS total,
  (SUM(CAST(cost AS NUMERIC))
    + SUM(IFNULL((SELECT SUM(CAST(c.amount AS NUMERIC))
                  FROM UNNEST(credits) AS c), 0)))
    AS total_exact
FROM `gcp-data-eng-374308.analysis.gcp_billing_export_v1_01CBD1_B38C45_E20EEB`
GROUP BY 1
ORDER BY 1 ASC;

/************** Total uasge cost per month group by Service  without Credits  *****************/

SELECT
  invoice.month,
  service.description as service,
  SUM(cost) AS total,
  (SUM(CAST(cost AS NUMERIC))) AS total_exact
FROM `gcp-data-eng-374308.analysis.gcp_billing_export_v1_01CBD1_B38C45_E20EEB`
GROUP BY 1,2
ORDER BY 1 ASC;

/************** Total uasge cost per month group by Service  with Credits  *****************/

SELECT
  invoice.month,
  service.description as service,
  SUM(cost)
    + SUM(IFNULL((SELECT SUM(c.amount)
                  FROM UNNEST(credits) c), 0))
    AS total,
  (SUM(CAST(cost AS NUMERIC))
    + SUM(IFNULL((SELECT SUM(CAST(c.amount AS NUMERIC))
                  FROM UNNEST(credits) AS c), 0)))
    AS total_exact
FROM `gcp-data-eng-374308.analysis.gcp_billing_export_v1_01CBD1_B38C45_E20EEB`
GROUP BY 1,2
ORDER BY 1 ASC;


/************** Total uasge cost for a perticular service  *****************/

SELECT
  SUM(cost) AS cost_before_credits,
  labels.value AS cluster_name
FROM  `gcp-data-eng-374308.analysis.gcp_billing_export_resource_v1_01CBD1_B38C45_E20EEB`
LEFT JOIN UNNEST(labels) as labels
  ON labels.key = "goog-k8s-cluster-name"
GROUP BY labels.value;

