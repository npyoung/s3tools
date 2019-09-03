from contextlib import contextmanager
from glob import glob
from io import BytesIO, StringIO
import logging
from tempfile import gettempdir
import os

import boto3
from tifffile import imread, imsave


_bucket = None
_s3 = boto3.resource('s3')

logging.getLogger("botocore.vendored.requests.packages.urllib3.connectionpool").setLevel(logging.WARNING)
logging.getLogger("tifffile").setLevel(logging.ERROR)

def set_bucket(bucket):
    global _bucket
    _bucket = bucket

def default_bucket():
    global _bucket
    if _bucket:
        return _bucket

    try:
        return os.environ['S3TOOLS_BUCKET']
    except KeyError as e:
        print("No bucket provided and neither `s3tools.set_bucket()` nor `S3TOOLS_BUCKET` have been set.")
        print("Please provide a way to determine the S3 bucket")
        raise e


def parse_url(url):
    # Full S3 URL provided
    if url[:5] == "s3://":
        parts = url[5:].split("/", maxsplit=1)
        bucket = parts[0]
        key = parts[1]
    # Fall back on default bucket
    else:
        bucket = default_bucket()
        key = url
    return _s3.Bucket(bucket), key


def get_keys(prefix):
    provided_prefix = prefix
    bucket, prefix = parse_url(prefix)
    keys = [o.key for o in bucket.objects.filter(Prefix=prefix)]
    if provided_prefix[:5] == "s3://":
        return ["s3://" + bucket.name + "/" + key for key in keys]
    else:
        return keys

def s3open(url_or_key, mode='rb', backend='memory'):
    if backend == 'memory':
        if mode == 'rb':
            return S3ReadBuffer(url_or_key)
        elif mode == 'wb':
            return S3WriteBuffer(url_or_key)
        else:
            raise ValueError("Only modes 'rb' and 'wb' supported")
    elif backend == 'file':
        bucket, key = parse_url(url_or_key)
        fname = key.rsplit("/", 1)[-1]
        fname = os.path.join(gettempdir(), fname)
        bucket.download_file(key, fname)
        return open(fname, mode='rb')
    else:
        raise ValueError("Backend must be one of 'memory' or 'file'")

@contextmanager
def S3ReadBuffer(url_or_key):
    bucket, key = parse_url(url_or_key)

    with BytesIO() as buff:
        bucket.download_fileobj(key, buff)
        buff.seek(0)
        yield buff

@contextmanager
def S3WriteBuffer(url_or_key):
    bucket, key = parse_url(url_or_key)

    with BytesIO() as buff:
        yield buff
        buff.seek(0)
        bucket.upload_fileobj(buff, key)

def get_s3_file(key):
    with s3open(key, 'rb') as buff:
        return buff.read()

def get_s3_img(key, backend='memory'):
    with s3open(key, 'rb', backend=backend) as buff:
        img = imread(buff)
    return img

def put_s3_file(key, text):
    with s3open(key, 'wb') as buff:
        buff.write(text)

def put_s3_img(key, img):
    with s3open(key, 'wb') as buff:
        imsave(buff, img, compress=6)

def expand_pattern(filename):
    if filename[:3] == "s3:":
        return get_keys(prefix)
    else:
        return glob(filename)
