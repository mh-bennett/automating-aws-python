import boto3
import sys
import click
from botocore.exceptions import ClientError
# pylint: disable=E1101
# ^ Done due to an incompatibility between pylint and Boto3 Dynamic typing
session = boto3.Session(profile_name='mb-aws-acct-cli')
s3 = session.resource('s3')

@click.group()
def cli():
    # Wraps around child click commands
    "Webotron deploys websites to AWS"
    pass 

@cli.command('list-buckets')
def list_buckets():
    "List all S3 buckets"
    for bucket in s3.buckets.all():
        print(bucket)

@cli.command('list-bucket-objects')
@click.argument('bucket')
def list_bucket_objects(bucket):
    "List objects in an S3 bucket"
    for obj in s3.Bucket(bucket).objects.all():
        print(obj)

@cli.command('setup-bucket')
@click.argument('bucket')
def setup_bucket(bucket):
    "Create and Configure an S3 bucket for static website hosting"
    s3_bucket = None
    try: 
        s3_bucket = s3.create_bucket(
            Bucket=bucket, 
            CreateBucketConfiguration={'LocationConstraint' : session.region_name}
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            s3_bucket =  s3.Bucket(bucket)
        else:
            raise e

    policy = """
    {
        "Version":"2012-10-17",
        "Statement":[{
            "Sid":"PublicReadGetObject",
            "Effect":"Allow",
            "Principal": "*",
            "Action":["s3:GetObject"],
            "Resource":["arn:aws:s3:::%s/*"]
        }]
    }
    """ % s3_bucket.name
    policy =  policy.strip()
    pol = s3_bucket.Policy()
    pol.put(Policy=policy)

    web = s3_bucket.Website()
    web.put(WebsiteConfiguration={
        'ErrorDocument': {
            'Key': 'error.html'
        },
        'IndexDocument': {
            'Suffix': 'index.html'
        }
    })

    return



if __name__ == "__main__":
    cli()
