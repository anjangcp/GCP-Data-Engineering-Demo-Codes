'''
Author @ Anjan GCP Data Engineering

Install Spanner Client Libraries
pip install google-cloud-spanner==3.31.0

Note: This is only for Educational purpose

These code samples will demo basic Cloud Spanner Operations
	1. Create Spanner Instance
	2. Create Spanner Database (Google Standard SQL), Table
	3. Insert data using DML statements
	4. Query Spanner data
    
'''

from google.cloud import spanner 

# Function to create  Spanner Instance
def create_instance(instance_id,region):
    """Creates an instance."""
    spanner_client = spanner.Client()

    config_name = "{}/instanceConfigs/regional-{}".format(
        spanner_client.project_name,region
    )

    instance = spanner_client.instance(
        instance_id,
        configuration_name=config_name,
        display_name="Demo Instance.",
        node_count=1
    )

    instance.create()

    print("Waiting for operation to complete...")
    print("Created instance {}".format(instance_id))

# Function to create  Spanner Database and Tables
def create_database(instance_id, database_id):
    """Creates a database and tables for demo data."""
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)

    database = instance.database(
        database_id,
        ddl_statements=[
            """CREATE TABLE employee (
            empid     INT64 NOT NULL,
            empname    STRING(1024),
            salary FLOAT64
        ) PRIMARY KEY (empid)"""
        ],
    )

    database.create()

    print("Waiting for operation to complete...")
    print("Created database {} on instance {}".format(database_id, instance_id))

# Function to insert data into Spanner database Table
def insert_data(instance_id, database_id):
    #Inserts sample data into the given database.

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.batch() as batch:
        batch.insert(
            table="employee",
            columns=("empid", "empname", "salary"),
            values=[
                (1, "Marc", 2032.5),
                (2, "Catalina", 1298.3),
                (3, "Alice", 3087.5),
                (4, "Lea", 1567.9),
                (5, "David", 2224.6),
            ],
        )
    print("Inserted data.")


# Function to query data from Spanner Database Table
def query_data(instance_id, database_id):
    """Queries sample data from the database using SQL."""
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            "SELECT empid,empname,salary AlbumTitle FROM employee"
        )

        for row in results:
            print("Emp ID: {}, Emp Name: {}, Salary: {}".format(*row))
            
# Create Spanner instance
create_instance('gcp-dataeng-demo','asia-south1')

#Create database and Table
create_database('gcp-dataeng-demo','demo_db')

# Insert data
insert_data('gcp-dataeng-demo','demo_db')

# Query data
query_data('gcp-dataeng-demo','demo_db')

# Delete Instance
# gcloud spanner instances delete gcp-dataeng-demo
