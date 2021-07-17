import boto3
from botocore.exceptions import ClientError
from functools import wraps
import json
import pandas as pd
import pickle

import os
import io
import re
import sklearn.preprocessing 
from time import time, sleep


def function_timer(func):
    
    @wraps(func)
    def _time_it(*args, **kwargs):
        start = int(round(time() * 1000))
        
        try:
            return func(*args, **kwargs)
        finally:
            end_ = round((int(round(time() * 1000)) - start) / 1000 / 60)
            print(f"Total execution time: {end_ if end_ > 0 else 0} minutes")
    return _time_it


class S3Utils():

    def __init__(self,
                 bucket_name, 
                 region = None):
        """
        Initialization of the object.

        Arguments:
        ----------
        bucket_name : str
            Name of the AWS S3 bucket the S3 object will refer to.
        """
        
        self.bucket_name = bucket_name
        if region is None:
            self.region_name = boto3.Session().region_name
        else:
            self.region_name = region
            
        self.s3 = boto3.client('s3', region_name = self.region_name)
        self.bucket = boto3.resource('s3', region_name = self.region_name).Bucket(bucket_name)

        
    def bucket_content_keys(self, directory=False):
        """
        Fetch all the files (or the subset in a specific directory) stored in the AWS S3 bucket.

        Parameters
        ----------
        directory : str, optional
            Path to the directory you want fetch files from, by default None.

        Returns
        -------
        list of str
            List of the desired file names.
        """
        
        if directory:
            keys = []
            for s3objs in self.bucket.objects.filter(Prefix=directory):
                keys.append(s3objs.key)
        else:
            keys = []
            for s3objs in self.bucket.objects.all():
                keys.append(s3objs.key)

        return keys
    
    
    def upload_file(self, file_name, object_name=None):
        """Upload a file to an S3 bucket
        
        Parameters
        ----------
        file_name: str, 
            File to upload
        object_name: str,
            S3 object name. If not specified then file_name is used
            
        Returns
        -------
        return: boolean
            Tru if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        # Upload the file
        try:
            response = self.s3.upload_file(file_name, self.bucket_name, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True
    
    
    def download_file(self, object_name, file_name=None):
        """Upload a file to an S3 bucket
        
        Parameters
        ----------
        file_name: str,
            output filename
        object_name: str,
            S3 object name. If not specified then file_name is used
            
        Returns
        -------
        return: boolean
            Tru if file was downloaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if file_name is None:
            file_name = object_name

        # Upload the file
        try:
            response = self.s3.download_file(self.bucket_name, object_name, file_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True
    


def athena_to_pandas(query, workgroup, output_bucket, staging_key, region = 'eu-west-1'):
    """
    Convenience function to start a query in Athena and 
    fetch the output in a pandas dataframe. Works with Pandas 1.0 or
    higher, but requires latest version of s3fs (0.4.2 or higher).
    For more info s3fs: https://pypi.org/project/s3fs/
    
    Parameters
    ----------
    
    query: str
        Sql query to execute
    workgroup: str
        business workgroup
    output_bucket: str
        bucket where to store the output
    staging_key: str
        specific path within the bucket where storing the output
    
    """
    
    client = boto3.client('athena', region_name= region)
    output_loc = os.path.join('s3://', os.path.join(output_bucket, staging_key))
    
    # Start Query
    response = client.start_query_execution(QueryString = query, 
                                        ResultConfiguration = {'OutputLocation':  output_loc}, 
                                        WorkGroup = workgroup)
   
    
    # Check Response
    check_response = True
    output_location = None

    while check_response:

        sleep(0.2)

        res = client.get_query_execution(
            QueryExecutionId= response['QueryExecutionId'])

        if res['QueryExecution']['Status']['State'] == 'SUCCEEDED':
            check_response = False
            output_location = res['QueryExecution']['ResultConfiguration']['OutputLocation']
            print("Execution Succeded")
            return pd.read_csv(output_location)
        elif res['QueryExecution']['Status']['State'] in ('RUNNING', 'QUEUED'):
            check_response = True
        else:
            print('Query Failed')
            print(res['QueryExecution']['Status']['StateChangeReason'])
            break
            
