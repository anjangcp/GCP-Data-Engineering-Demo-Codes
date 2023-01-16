-- List all tables in a database.
SELECT * FROM EXTERNAL_QUERY("projects/gcp-data-eng-374308/locations/asia-south1/connections/cloudsql_connect",
"select * from information_schema.tables;");

-- List all columns in a table.
SELECT * FROM EXTERNAL_QUERY("projects/gcp-data-eng-374308/locations/asia-south1/connections/cloudsql_connect",
"select * from information_schema.columns where table_name='product_master';");


-- Query data from CloudSQL table.
SELECT * FROM EXTERNAL_QUERY("projects/gcp-data-eng-374308/locations/asia-south1/connections/cloudsql_connect",
"select * from product_master;");

--Join Bigquery table with Cloud SQL table

SELECT  pm.product_name,
        pm.product_desc,
        dtls.qty,
        dtls.price,
        dtls.date
FROM `gcp-data-eng-374308.federated_demo.product_sales_dtls` dtls 
JOIN 
(
  SELECT * 
  FROM EXTERNAL_QUERY("projects/gcp-data-eng-374308/locations/asia-south1/connections/cloudsql_connect",
                      "select * from product_master;")
) pm
ON dtls.product_code = pm.product_code;

