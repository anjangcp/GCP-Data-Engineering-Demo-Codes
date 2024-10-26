""" 

Author : @ Anjan GCP Data Engineering

Created for educational purpose only ...

This python file has different python functions to explain 
    1. Create GCS bucket
    2. Manage GCS bucket
    3. Upload Storage objects
    4. Download Storage objects
    5. Bulk uploads and downloads
    6. Delete bucket
using Python GCS clinet libraries

Insallations
pip3 install google-cloud-storage

"""

from google.cloud import storage

""" Create Bucket """

bucket_name = "demo_gcp_dataeng123"

def create_bucket_class_location(bucket_name):
    """
    Create a new bucket in the US region with the coldline storage
    class
    """
    # bucket_name = "your-new-bucket-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "STANDARD"
    new_bucket = storage_client.create_bucket(bucket, location="us")

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    return new_bucket

create_bucket_class_location(bucket_name)

""" List Buckets """ 

def list_buckets():

    storage_client = storage.Client()
    buckets = storage_client.list_buckets()

    for bucket in buckets:
        print(bucket.name)

#list_buckets()

""" Get Bucket metadata info """

bucket_name = "demo_gcp_dataeng123"

def bucket_metadata(bucket_name):
    """Prints out a bucket's metadata."""
    # bucket_name = 'your-bucket-name'

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    print(f"ID: {bucket.id}")
    print(f"Name: {bucket.name}")
    print(f"Storage Class: {bucket.storage_class}")
    print(f"Location: {bucket.location}")
    print(f"Location Type: {bucket.location_type}")
    print(f"Cors: {bucket.cors}")
    print(f"Default Event Based Hold: {bucket.default_event_based_hold}")
    print(f"Default KMS Key Name: {bucket.default_kms_key_name}")
    print(f"Metageneration: {bucket.metageneration}")
    print(
        f"Public Access Prevention: {bucket.iam_configuration.public_access_prevention}"
    )
    print(f"Retention Effective Time: {bucket.retention_policy_effective_time}")
    print(f"Retention Period: {bucket.retention_period}")
    print(f"Retention Policy Locked: {bucket.retention_policy_locked}")
    print(f"Object Retention Mode: {bucket.object_retention_mode}")
    print(f"Requester Pays: {bucket.requester_pays}")
    print(f"Self Link: {bucket.self_link}")
    print(f"Time Created: {bucket.time_created}")
    print(f"Versioning Enabled: {bucket.versioning_enabled}")
    print(f"Labels: {bucket.labels}")
    
#bucket_metadata(bucket_name)

bucket_name = "demo_gcp_dataeng123"
source_file_name = "/home/gcpdataeng36/input_batch_data.csv"
destination_blob_name = "input_batch_data.csv" 


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )
#upload_blob(bucket_name, source_file_name, destination_blob_name)


""" Get object's ACLs """

bucket_name = "demo_gcp_dataeng123"
blob_name = 'input_batch_data.csv'
def print_blob_acl(bucket_name, blob_name):
    """Prints out a blob's access control list."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    for entry in blob.acl:
        print(f"{entry['role']}: {entry['entity']}")

#print_blob_acl(bucket_name, blob_name)

"""  List all objcet in a bucket """

bucket_name = "demo_gcp_dataeng123"

def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    # Note: The call returns a response only when the iterator is consumed.
    for blob in blobs:
        print(blob.name)

#list_blobs(bucket_name)

""" Upload multiple objects with transfer manager in parallel """

bucket_name = "demo_gcp_dataeng123"
filenames = ["demo1.mov","input_batch_data.csv","ind_niftyrealtylist.csv"]
source_directory="/home/gcpdataeng36"
workers=8

def upload_many_blobs_with_transfer_manager(
    bucket_name, filenames, source_directory, workers
):
    """Upload every file in a list to a bucket, concurrently in a process pool.

    Each blob name is derived from the filename, not including the
    `source_directory` parameter. For complete control of the blob name for each
    file (and other aspects of individual blob metadata), use
    transfer_manager.upload_many() instead.
    """

    from google.cloud.storage import Client, transfer_manager
    import datetime

    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)

    print("start time:",datetime.datetime.now())

    results = transfer_manager.upload_many_from_filenames(
        bucket, filenames, source_directory=source_directory, max_workers=workers
    )

    for name, result in zip(filenames, results):
        # The results list is either `None` or an exception for each filename in
        # the input list, in order.

        if isinstance(result, Exception):
            print("Failed to upload {} due to exception: {}".format(name, result))
        else:
            print("Uploaded {} to {}.".format(name, bucket.name))
    print("end time:",datetime.datetime.now())

#upload_many_blobs_with_transfer_manager(bucket_name, filenames, source_directory, workers)

""" Upload large files in chunks """

bucket_name = "demo_gcp_dataeng123"
source_filename = "/home/gcpdataeng36/demo1.mov"
destination_blob_name = "demo1.mov"
workers=8

def upload_chunks_concurrently(
    bucket_name,
    source_filename,
    destination_blob_name,
    chunk_size=32 * 1024 * 1024,
    workers=8,
):
    """Upload a single file, in chunks, concurrently in a process pool."""

    from google.cloud.storage import Client, transfer_manager
    import datetime

    print("start time:",datetime.datetime.now())
    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    transfer_manager.upload_chunks_concurrently(
        source_filename, blob, chunk_size=chunk_size, max_workers=workers
    )

    print(f"File {source_filename} uploaded to {destination_blob_name}.")
    print("end time:",datetime.datetime.now())

#upload_chunks_concurrently(bucket_name,source_filename,destination_blob_name,chunk_size=32 * 1024 * 1024,workers=8)

""" Download multiple files """

bucket_name = "demo_gcp_dataeng123"
blob_names = ["demo1.mov","input_batch_data.csv","ind_niftyrealtylist.csv"]
destination_directory =  "/home/gcpdataeng36/downloads"
workers=8

def download_many_blobs_with_transfer_manager(
    bucket_name, blob_names, destination_directory, workers=8
):
    """Download blobs in a list by name, concurrently in a process pool.

    The filename of each blob once downloaded is derived from the blob name and
    the `destination_directory `parameter. For complete control of the filename
    of each blob, use transfer_manager.download_many() instead.

    Directories will be created automatically as needed to accommodate blob
    names that include slashes.
    """
    from google.cloud.storage import Client, transfer_manager

    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)

    results = transfer_manager.download_many_to_path(
        bucket, blob_names, destination_directory=destination_directory, max_workers=workers
    )

    for name, result in zip(blob_names, results):
        # The results list is either `None` or an exception for each blob in
        # the input list, in order.

        if isinstance(result, Exception):
            print("Failed to download {} due to exception: {}".format(name, result))
        else:
            print("Downloaded {} to {}.".format(name, destination_directory + name))


#download_many_blobs_with_transfer_manager(bucket_name, blob_names, destination_directory, workers=8)

""" delete bucket """

bucket_name = "demo_gcp_dataeng123"

def delete_bucket(bucket_name):
    """Deletes a bucket. The bucket must be empty."""
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)
    bucket.delete()

    print(f"Bucket {bucket.name} deleted")

#delete_bucket(bucket_name)
