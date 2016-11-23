# -*- coding:utf-8 -*-

import sys
import os
import yaml
import zipfile
import pandas
import numpy
import boto3


reload(sys)
sys.setdefaultencoding('utf8')
curdir = os.path.abspath(os.path.curdir)

yaml_obj = yaml.load(open('item-bill.yaml'))
data_dir = 'data'
data_dir = os.path.join(curdir, data_dir)
tmp_dir = 'tmp'
tmp_dir = os.path.join(curdir, tmp_dir)

bills = yaml_obj.get('s3prefix')
bills_prefix = bills['bill-prefix']
raw_prefix = bills['raw-prefix']
proc_prefix = bills['proc-prefix']

tags = yaml_obj.get('usertags')
tagprefix = tags['prefix']
taglist = tags['tags']
billTags = []
for tag in taglist:
    print tagprefix+':'+tag['tag']
    billTags.append(tagprefix+':'+tag['tag'])

projectTag = 'user:Project'
ztjyTag = 'Shanghai Edu ZTJY'

session = boto3.Session(profile_name='default')
# Any clients created from this session will use credentials
# from the [bill] section of ~/.aws/credentials.
client = session.client('s3')
response = client.list_buckets()

s3 = session.resource('s3')

# Print out bucket names
for bucket in s3.buckets.all():
    print(bucket.name)

bill_bucket = s3.Bucket('edusoft-aws-bill')
proc_bucket = s3.Bucket('edusoft-aws-bill-work')

object_summary_iterator = bill_bucket.objects.all()

firstCSV = True
dframe = pandas.read_csv(os.path.join(data_dir, 'headers.csv'), dtype=object, low_memory=False)

for obj_i in object_summary_iterator:
    if obj_i.key.find(bills_prefix) > -1:
        obj = s3.Object(obj_i.bucket_name, obj_i.key)
        tmpfile = os.path.join(tmp_dir, obj.key)
        obj.download_file(tmpfile)

        zFile = zipfile.ZipFile(tmpfile,'r')
        for filename in zFile.namelist():
            print filename[len(bills_prefix):]
            data = zFile.read(filename)
            file = open(os.path.join(tmp_dir, filename[len(bills_prefix):]),'w+b')
            file.write(data)
            file.close()
            data = pandas.read_csv(file.name, dtype=object, low_memory=False)
            df = data[(data['RecordType'] == 'LineItem')]

            columns = list(df.columns.values)
            for tag in billTags:
                if not( tag in columns):
                    if tag == projectTag:
                        df[tag] = df.apply(lambda _: ztjyTag, axis=1)
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
                null_rate = sum_total/(sum_total-sum_null)
            df['NullRate'] = df.apply(lambda _: null_rate, axis=1)

            df.to_csv( os.path.join(data_dir, filename[len(bills_prefix):]), index=False)

            if firstCSV:
                dframe = df.copy()
                df_tmp = dframe
                firstCSV = False
            else:
                data = [df_tmp, df]
                dframe = pandas.concat(data)
                df_tmp = dframe

        os.remove(tmpfile)
        print df_tmp.shape
        #break

df.to_csv( os.path.join(data_dir, 'current-bill.csv'), index=False)

df.head(1).to_csv(os.path.join(data_dir, 'headers.csv'), index=False)

dframe.to_csv( os.path.join(data_dir, 'history-bill.csv'), index=False)


