from contextlib import contextmanager
from glob import glob
from io import BytesIO, StringIO
import os
import warnings

import boto3
from tifffile import imread, imsave


_bucket = None
_s3 = boto3.resource('s3')

def set_bucket(bucket):
    _bucket = bucket

def default_bucket():
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
    bucket, prefix = parse_url(prefix)
    return [o.key for o in bucket.objects.filter(Prefix=prefix)]

def s3open(url_or_key, mode='rb'):
    if mode == 'rb':
        return S3ReadBuffer(key)
    elif mode == 'wb':
        return S3WriteBuffer(key)
    else:
        raise ValueError("Only modes 'rb' and 'wb' supported")

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

def get_s3_img(key):
    with s3open(key, 'rb') as buff:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message='ome-xml')
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