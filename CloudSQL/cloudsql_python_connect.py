'''
Installations required -
pip install cloud-sql-python-connector["pymysql"] SQLAlchemy
'''
#Import required dependencies
from google.cloud.sql.connector import Connector
import sqlalchemy

# initialize Connector object
connector = Connector()

# function to return the database connection
def getconn():
    conn= connector.connect(
        "gcp-data-eng-374308:asia-south1:sql-demo",
        "pymysql",
        user="root",
        password="anjan",
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
    db_conn.execute("DROP TABLE basic_dtls")
