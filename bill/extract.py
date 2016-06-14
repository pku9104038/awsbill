# -*- coding: utf-8 -*-


import os
import yaml
import boto3

class ProcessingEnv(object):
    def __init__(self, curdir):

        __yaml_obj = yaml.load(open(os.path.join(curdir, 'item-bill.yaml')))
        __dir = __yaml_obj.get('dir')
        __data = __dir['data']
        __tmp = __dir['tmp']
        self.data_dir = os.path.join(curdir, __data)
        self.tmp_dir = os.path.join(curdir, __tmp)

        __s3prefix = __yaml_obj.get('s3prefix')
        self.bills_prefix = __s3prefix['bill-prefix']
        self.invoice_prefix = __s3prefix['invoice-prefix']
        self.estimate_prefix = __s3prefix['estimate-prefix']
        self.processed_bills = __s3prefix['processed-bills']

        __tags = __yaml_obj.get('usertags')
        __tagprefix = __tags['prefix']
        __taglist = __tags['tags']
        self.billTags = []
        for __tag in __taglist:
            print __tagprefix + ':' + __tag['tag']
            self.billTags.append(__tagprefix + ':' + __tag['tag'])

        self.projectTag = __tags['projectTag']
        self.ztjyTag = __tags['ztjyTag']

        __s3buckets = __yaml_obj.get('s3buckets')
        __bill_bucket_name = __s3buckets['bill']
        __work_bucket_name = __s3buckets['work']

        __profile_name=__yaml_obj.get('aws-profile')
        self.session = boto3.Session(__profile_name)
        self.client = self.session.client('s3')
        self.s3 = self.session.resource('s3')
        self.bill_bucket = self.s3.Bucket(__bill_bucket_name)
        self.proc_bucket = self.s3.Bucket(__work_bucket_name)
