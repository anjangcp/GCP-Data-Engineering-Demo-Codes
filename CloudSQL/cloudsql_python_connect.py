'''
Installations required -
pip install cloud-sql-python-connector["pymysql"] SQLAlchemy
pip install google-cloud-secret-manager
'''
#Import required dependencies
from google.cloud.sql.connector import Connector
import sqlalchemy

# Function to get CloudSQL instance password from Secret Manager
def access_secret_version(project_id, secret_id, version_id):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """

    # Import the Secret Manager client library.
    from google.cloud import secretmanager

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})
    # Print the secret payload.
    # snippet is showing how to access the secret material.
    payload = response.payload.data.decode("UTF-8")
    return payload

# Function call to get DB password ino a local varaiable  
db_password = access_secret_version('gcp-data-eng-374308', 'cloudsql_pwd','1')


# initialize Connector object
connector = Connector()

# function to return the database connection
def getconn():
    conn= connector.connect(
        "gcp-data-eng-374308:asia-south1:sql-demo",
        "pymysql",
        user="root",
        password=db_password,
        db="gcp_demo"
    )
    return conn
# create connection pool
pool = sqlalchemy.create_engine(
    "mysql+pymysql://",
    creator=getconn,
)

# insert statement (DML statement for data load)
insert_stmt = sqlalchemy.text(
    "INSERT INTO basic_dtls (idn, name) VALUES (:idn, :name)",
)

# interact with Cloud SQL database using connection pool
with pool.connect() as db_conn:
    
    # Create Table
    db_conn.execute("CREATE TABLE basic_dtls(idn INT, name VARCHAR(200))")

    # Insert data into Table
    
    db_conn.execute(insert_stmt, idn=1, name="AAA")
    db_conn.execute(insert_stmt, idn=2, name="BBB")
    db_conn.execute(insert_stmt, idn=3, name="CCC")


    # query database
    result = db_conn.execute("SELECT * from basic_dtls").fetchall()

    # Do something with the results
    for row in result:
        print(row)

    # Dropping Table
    #db_conn.execute("DROP TABLE basic_dtls")
