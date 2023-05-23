-- Create Table GoogleSQL
CREATE TABLE employee (
  idn INT64,
  name STRING(MAX),
  salary FLOAT64,
) PRIMARY KEY(idn);

--Insert data
insert into employee(idn,name,salary) 
values(1,'aa',100.5),(2,'bb',1200.0);


-- Create Table PostgresSQL
CREATE TABLE employee (
  idn bigint NOT NULL,
  name character varying(256),
  salary numeric,
  PRIMARY KEY(idn)
);

--CLI create instance
gcloud spanner instances create gcp-demo-instance --config=regional-us-central1 \
    --description="Demo Instance" --nodes=1

-- Set Instance
gcloud config set spanner/instance gcp-demo-instance

--Create database
gcloud spanner databases create example-db

-- Create table
gcloud spanner databases ddl update example-db \
  --ddl='CREATE TABLE Books ( BookId INT64 NOT NULL, 
                                BookName STRING(1024), 
                                BookCatgry STRING(1024)) PRIMARY KEY (BookId)'
-- Insert data                              
gcloud spanner rows insert --database=example-db \
      --table=Books \
      --data=BookId=1,BookName=abc,BookCatgry=Finance
      
gcloud spanner rows insert --database=example-db \
      --table=Books \
      --data=BookId=2,BookName=aaa,BookCatgry=Comic
      
gcloud spanner rows insert --database=example-db \
      --table=Books \
      --data=BookId=3,BookName=ccc,BookCatgry=History

-- Query data
gcloud spanner databases execute-sql example-db \
    --sql='SELECT * FROM Books'
 
 -- Delete database   
gcloud spanner databases delete example-db

-- Delete Instance
gcloud spanner instances delete gcp-demo-instance

