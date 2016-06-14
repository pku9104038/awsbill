# -*- coding: utf-8 -*-


import os
import yaml
import boto3
import zipfile
import pandas
import numpy

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

def extractZipBill(procenv,bucket_name, object_key):
    obj = procenv.s3.Object(bucket_name, object_key)
    tmpfile = os.path.join(procenv.tmp_dir, obj.key)
    obj.download_file(tmpfile)
    zFile = zipfile.ZipFile(tmpfile, 'r')
    for filename in zFile.namelist():
        print filename[len(procenv.bills_prefix):]
        data = zFile.read(filename)
        file = open(os.path.join(procenv.tmp_dir, filename[len(procenv.bills_prefix):]), 'w+b')
        file.write(data)
        file.close()
        data = pandas.read_csv(file.name, dtype=object, low_memory=False)
        df = data[(data['RecordType'] == 'LineItem')]

        columns = list(df.columns.values)
        for tag in procenv.billTags:
            if not (tag in columns):
                if tag == procenv.projectTag:
                    df[tag] = df.apply(lambda _: procenv.ztjyTag, axis=1)
                else:
                    df[tag] = df.apply(lambda _: numpy.nan, axis=1)

        df_null = df[(df['user:Project'].isnull())]
        df_totalcost = df['UnBlendedCost']
        df_totalcost = pandas.to_numeric(df_totalcost)
        df_nullcost = df_null['UnBlendedCost']
        df_nullcost = pandas.to_numeric(df_nullcost)
        sum_total = df_totalcost.sum()
        sum_null = df_nullcost.sum()
        if sum_null == 0:
            null_rate = 1
        else:
            null_rate = sum_total / (sum_total - sum_null)

        df['NullRate'] = df.apply(lambda _: null_rate, axis=1)

        df.to_csv(os.path.join(procenv.data_dir, filename[len(procenv.bills_prefix):]), index=False)
        """
        if firstCSV:
            dframe = df.copy()
            df_tmp = dframe
            firstCSV = False
        else:
            data = [df_tmp, df]
            dframe = pandas.concat(data)
            df_tmp = dframe
        """
    os.remove(tmpfile)