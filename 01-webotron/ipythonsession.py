import boto3
session = boto3.Session(profile_name='mb-aws-acct-cli')
session.resource('s3')
