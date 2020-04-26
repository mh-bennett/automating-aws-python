#!/usr/bin/python
# -*- coding: utf-8 -*-
# pylint: disable=E1101
"""
This is the Webotron Script - it takes CLI arguments to set up S3 websites.
"""

import click
import boto3

from bucket_manager import BucketManager

SESSION = None
BUCKET_MANAGER = None

@click.group()
@click.option('--profile', default=None,
    help="Use a given AWS profile")
def cli(profile):
    """
    Webotron deploys websites to AWS.
    """
    global SESSION, BUCKET_MANAGER
    session_cfg = {}
    if profile:
        session_cfg['profile_name'] = profile
        print('Running with the {} AWS profile'.format(profile))
    SESSION = boto3.Session(**session_cfg) 
    # **glob <- this is a extensible set of params that maps to a dict.
    BUCKET_MANAGER = BucketManager(SESSION)


@cli.command('list-buckets')
def list_buckets():
    """
    List all S3 buckets.
    """
    for bucket in BUCKET_MANAGER.list_buckets():
        print(bucket)


@cli.command('list-bucket-objects')
@click.argument('bucket')
def list_bucket_objects(bucket):
    """
    List objects in an S3 bucket.
    """
    for obj in BUCKET_MANAGER.list_bucket_objects(bucket):
        print(obj)


@cli.command('setup-bucket')
@click.argument('bucket')
def setup_bucket(bucket):
    """
    Create and Configure an S3 bucket for static website hosting.
    """
    s3_bucket = BUCKET_MANAGER.init_bucket(bucket)
    BUCKET_MANAGER.set_bucket_policy(s3_bucket)
    BUCKET_MANAGER.allow_website(s3_bucket)


@cli.command('sync')
@click.argument('pathname', type=click.Path(exists=True))
@click.argument('bucket')
def sync(bucket, pathname):
    """
    CLI Argument to Sync Files into a specified S3 Bucket.
    """
    BUCKET_MANAGER.sync(pathname, bucket)
    print(BUCKET_MANAGER.get_bucket_url(BUCKET_MANAGER.s3.Bucket(bucket)))


if __name__ == "__main__":
    cli()
