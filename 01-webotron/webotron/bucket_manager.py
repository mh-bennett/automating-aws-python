# -*- coding: utf-8 -*-
"""
bucket_manager.py
"""
from pathlib import Path
import mimetypes
import boto3
from botocore.exceptions import ClientError
from functools import reduce
from hashlib import md5
import util


class BucketManager():
    """
    Interface for Boto3 S3 bucket API
    """
    CHUNK_SIZE = 8388608

    def __init__(self, session):
        """
        This is the class __init__ function.
        It initializes class variables to allow interface with S3.
        Params: session obj
        """
        self.session = session
        self.s3 = session.resource('s3')
        self.transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_chunksize=self.CHUNK_SIZE,
            multipart_threshold=self.CHUNK_SIZE
        )
        self.manifest = {}


    def list_buckets(self):
        """
        List all S3 buckets.
        """
        buckets = self.s3.buckets.all()
        return buckets


    def list_bucket_objects(self, bucket):
        """
        List objects in an S3 bucket.
        Params: bucket (valid bucket)
        """
        objs = None
        try:
            objs = self.s3.Bucket(bucket).objects.all()
        except ClientError as error:
            print('{}'.format(error.response['Error']['Code']))

        return objs


    def init_bucket(self, bucket_name):
        """
        Initialize a bucket in S3.
        Params: bucket_name
        """
        s3_bucket = None
        try:
            s3_bucket = self.s3.create_bucket(
                Bucket=str(bucket_name),
                CreateBucketConfiguration={'LocationConstraint' : self.session.region_name}
            )
        except ClientError as error:
            if error.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                s3_bucket = self.s3.Bucket(bucket_name)
            else:
                raise error
        return s3_bucket

    def get_region_name(self, bucket):
        """Get the bucket's region name"""
        bucket_location = self.s3.meta.client.get_bucket_location(Bucket=bucket.name)
        return bucket_location["LocationConstraint"] or 'us-east-1'

    def get_bucket_url(self, bucket):
        """Get the website URL for this bucket."""
        return "http://{}.{}.".format(
            bucket.name, util.get_endpoint(
                self.get_region_name(bucket)).host)

    @staticmethod
    def set_bucket_policy(bucket):
        """
        Applies a public read-only bucket policy.
        This policy is intended to be used for static site hosting.
        Params: bucket (valid bucket)
        """
        policy = """
        {
          "Version":"2012-10-17",
          "Statement":[{
          "Sid":"PublicReadGetObject",
          "Effect":"Allow",
          "Principal": "*",
              "Action":["s3:GetObject"],
              "Resource":["arn:aws:s3:::%s/*"
              ]
            }
          ]
        }
        """ % bucket.name
        policy = policy.strip()
        try:
            pol = bucket.Policy()
            pol.put(Policy=policy)
        except ClientError as error:
            raise print('{}'.format(error.response['Error']['Code']))

    @staticmethod
    def allow_website(bucket):
        """
        Configure S3 website hosting for bucket.
        Modifies the S3 settings for a bucket to allow static site hosting.
        Params: bucket (valid bucket)
        """
        try:
            bucket.Website().put(WebsiteConfiguration={
                'ErrorDocument': {
                    'Key': 'error.html'
                },
                'IndexDocument': {
                    'Suffix': 'index.html'
                }
            })
        except ClientError as error:
            raise print('{}'.format(error.response['Error']['Code']))


    def gen_etag(self, path):
        """
        Generate etag for file.
        """
        hashes = [] 
        with open(path, 'rb') as f:
            while True:
                data = f.read(self.CHUNK_SIZE)
                if not data:
                    break
                hashes.append(self.hash_data(data))
            if not hashes:
                return    
            if len(hashes) == 1:
                return '"{}"'.format(hashes[0].hexdigest())
            else:
                hash = self.hash_data(reduce(lambda x, y: x + y, (h.digest() for h in hashes)))
                return '"{}-{}"'.format(hash.hexdigest(), len(hashes))


    def upload_file(self, bucket, path, key):
        """
        This static method is used to upload a file to S3.\
        It is based on the file path and file key passed as params
        (the key becomes the object name in S3).
        """
        content_type = mimetypes.guess_type(key)[0] or 'text/plain'
        etag = self.gen_etag(path)
        if self.manifest.get(key, '') == etag:
            print("Etag is the same for {}. No need to update.".format(key))
            return
        print("{} does not exist or was updated, uploading.".format(key))
        try:
            bucket.upload_file(
                path,
                key,
                ExtraArgs={
                    'ContentType': content_type
                },
                Config=self.transfer_config
            )
        except ClientError as error:
            print('{}'.format(error.response['Error']['Code']))
        return
            
    
    @staticmethod
    def hash_data(data):
        """
        Generate an md5 hash for the data.
        """
        hash = md5()
        hash.update(data)
        return hash


    def load_manifest(self, bucket):
        """Load Manifest for caching purposes."""
        self.manifest = {}
        paginator = self.s3.meta.client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket.name):
            for obj in page.get('Contents', []):
                self.manifest[obj['Key']] = obj['ETag']


    def sync(self, pathname, bucket_name):
        """
        This command syncs the contents of `PATHNAME` into a `BUCKET`.
        It checks the path passed into the function and strips it;
        this handles varying nomenclature for different
        operating systems and relative paths.
        """
        s3_bucket = self.s3.Bucket(bucket_name)
        print(self.manifest)
        self.load_manifest(s3_bucket)
        print(self.manifest)
        root = Path(pathname).expanduser().resolve()

        """
        This is a closure to recursively upload files to S3 within a directory to S3.
        It calls the static method `upload_file()` to upload to S3.
        """
        def handle_directory(target):
            for path in target.iterdir():
                if path.is_dir():
                    handle_directory(path)
                if path.is_file():
                    self.upload_file(s3_bucket, str(path), str(path.relative_to(root)))

        handle_directory(root)
