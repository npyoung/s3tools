from contextlib import contextmanager
from glob import glob
from io import BytesIO, StringIO
import warnings

import boto3
from tifffile import imread, imsave


BUCKET = 'xlfm-dcon'
_s3 = boto3.resource('s3')


def get_keys(prefix):
    bucket = _s3.Bucket(BUCKET)

    return [o.key for o in bucket.objects.filter(Prefix=prefix)]

def s3open(key, mode='rb'):
    if mode == 'rb':
        return S3ReadBuffer(key)
    elif mode == 'wb':
        return S3WriteBuffer(key)
    else:
        raise ValueError("Only modes 'rb' and 'wb' supported")

@contextmanager
def S3ReadBuffer(key):
    bucket = _s3.Bucket(BUCKET)
    
    with BytesIO() as buff:
        bucket.download_fileobj(key, buff)
        buff.seek(0)
        yield buff
        
@contextmanager
def S3WriteBuffer(key):
    bucket = _s3.Bucket(BUCKET)
    
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