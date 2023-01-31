'''
Author : @ Anjan GCP Data Engineering
Created SQLs required for Big Query Procedures and Anonymous Blocks Demo

This code is to demo , how to define and manage GCS Object LifeCycle Management rules using Python Client Libraries
'''

from google.cloud import storage

def enable_bucket_lifecycle_management(bucket_name):
    """Enable lifecycle management for a bucket"""

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)
    rules = bucket.lifecycle_rules

    # Print default Rules
    print(f"Lifecycle management rules for bucket {bucket_name} are {list(rules)}")

    # Add new Rules
    #bucket.add_lifecycle_delete_rule(age=2)

    #Clearing Rules
    bucket.clear_lifecyle_rules()
    bucket.patch()

    # Print Rules again after modifications
    rules = bucket.lifecycle_rules
    print(f"Lifecycle management is enable for bucket {bucket_name} and the rules are {list(rules)}")

    return bucket

enable_bucket_lifecycle_management('gcp-data-eng-demos')
