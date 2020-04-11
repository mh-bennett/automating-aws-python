import boto3
import sys
import click

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

if __name__ == "__main__":
    cli()
