# -*- coding:utf-8 -*-
#
# Script for aws bill data processing and statistics
# Download aws-billing-detailed-line-items-with-resources-and-tags zip file
# Unzip and add tags project tag for the first two bills from 2014.12

import boto3
import zipfile as zip
import pandas
import sys
import os
import getopt
import yaml
import numpy


# Define the usage helper and get options function

def usage():
    """
    print usage command help
    :return:
    """
    print "please usage:"
    print "python "+ sys.argv[0] +" [-s scope] + [-p profile] [-c config_yaml]"

    # for option -s
    print ""
    print "    -s: options for processing scope"
    print "         all: process all bills"
    print "         last: process the bill of last month"
    print "         latest: process latest bill"
    print "         yyyy-mm: process the bill of month: yyyy-mm"
    print "         default: latest"
    print ""

    # for option -p
    print ""
    print "    -p: setting aws access profile"
    print "         profile: the [profile] in .aws/config, .aws/credentials"
    print "         default: default"
    print ""

    # for option -c
    print ""
    print "    -c: setting configuration yaml file"
    print "         config_yaml: the .yaml file  work directory"
    print "         default: config.yaml"
    print ""

def getopts():
    """
    get options from command line parameters
    :return: scope
    """


    # set defaults
    opts = []
    args = []

    scope = "latest"
    profile = "default"
    config = "config2.yaml"

    # get options
    try:
        """
        try to get commandline options
        """
        opts,args = getopt.getopt(sys.argv[1:],"h:s:p:c:")
    except Exception as e:
        """
        process option error
        """
        print ("open exception: %s: %s\n" %(e.args, e.message))
        usage()
        sys.exit(1)

    # use default option if no command line input
    if len(opts) == 0:
        """
        process null option
        """
        print " "
        print "runnning default options: python "+ sys.argv[0] +" -s latest + -p default -c config2.yaml"
        print " "
        #usage()
        #sys.exit(1)
        # continue script with default options

    # set options according to command line input
    for op, value in opts:
        if op == "-s":
            scope = value
        elif op == "-p":
            profile = value
        elif op == "-c":
            config = value
        elif op == "-h":
            usage()
            sys.exit()

    return scope, profile, config

# Define script scope variables

class Config(object):
    """
    calss for process environments configurations
    """

    def __init__(self, scope, config_yaml):
        # init scope
        self.scope = scope

        # load config_yaml
        self.config_file = config_yaml
        self.yaml_obj = yaml.load(open(config_yaml))


        # get bill_logs
        bill_logs = self.yaml_obj.get("bill_logs")
        self.log_bucket = bill_logs["s3_bucket"]["bucket_name"]
        self.log_prefix = bill_logs["s3_bucket"]["log_prefix"]

        # get bill_processed
        bill_processed = self.yaml_obj.get("bill_processed")
        self.proc_bucket = bill_processed["s3_bucket"]["bucket_name"]
        self.raw_folder = bill_processed["s3_bucket"]["raw_folder"]
        self.tag_folder = bill_processed["s3_bucket"]["tag_folder"]
        self.cal_folder = bill_processed["s3_bucket"]["cal_folder"]
        self.stat_folder = bill_processed["s3_bucket"]["stat_folder"]
        self.raw_prefix = bill_processed["s3_bucket"]["raw_prefix"]
        self.tag_prefix = bill_processed["s3_bucket"]["tag_prefix"]
        self.cal_prefix = bill_processed["s3_bucket"]["cal_prefix"]
        self.stat_prefix = bill_processed["s3_bucket"]["stat_prefix"]

        # local directory
        directories = self.yaml_obj.get("directories")
        self.cwd = os.path.abspath(os.path.curdir)
        self.tmp_dir = os.path.join(self.cwd, directories["tmp"])
        self.data_dir = os.path.join(self.cwd, directories["data"])
        if(not os.path.exists(self.tmp_dir)):
            os.mkdir(self.tmp_dir)
        if (not os.path.exists(self.data_dir)):
            os.mkdir(self.data_dir)

        # tags
        self.bill_tags = self.yaml_obj.get("bill_tags")
        self.lost_tag = self.yaml_obj.get("lost_tag")


class AWS_Access(object):
    """
    calss for AWS_Access session, clients, resources
    """
    def __init__(self, profile, config):
        """
        init attributes
        """

        self.config = config
        self.profile = profile

        # init aws_session
        self.session = boto3.Session(profile_name=profile)

        # Any clients created from this session will use credentials
        # from the [profile] section of ~/.aws/credentials.

        # init s3_client, s3_resource
        self.s3_client = self.session.client('s3')
        self.s3_resource = self.session.resource('s3')

        # init buckets
        #self.log_bucket = self.s3_resource.Bucket(config.log_bucket)
        #self.proc_bucket = self.s3_resource.Bucket(config.proc_bucket)


    def get_logs(self):
        """

        :param scope: all, latest, last, yyyy-mm
        :return:
        """
        # item detail logs object list
        obj_logs = self.s3_client.list_objects(Bucket=self.config.log_bucket, \
                                    Prefix=self.config.log_prefix)["Contents"]

        # fetch item from the list according to the scope option
        objs = []
        scope = self.config.scope
        if(scope == "all"): # all
            objs = obj_logs
        elif(scope == "latest"): # latest
            obj = obj_logs[len(obj_logs)-1]
            objs.append(obj)
        elif(scope == "last"): # last
            obj = obj_logs[len(obj_logs) - 2]
            objs.append(obj)
        else: # yyyy-mm
            for obj in obj_logs:
                filename = obj["Key"]
                month = filename[len(self.config.log_prefix):len(self.config.log_prefix)+7]
                if (month == scope):
                    objs.append(obj)

        return objs

    def upzip_log(self,obj_key):
        """

        :param obj_key: the object key of bill log file
        :return:
        """

        # download bill log zip file
        obj = self.s3_resource.Object(self.config.log_bucket, obj_key)
        zipfile = os.path.join(self.config.tmp_dir, obj.key)
        obj.download_file(zipfile)

        # process zip file
        zFile = zip.ZipFile(zipfile, 'r')
        for filename in zFile.namelist():
            # unzip log csv file
            print filename[len(self.config.log_prefix):]
            data = zFile.read(filename)
            file = open(os.path.join(self.config.tmp_dir, \
                                     filename[len(self.config.log_prefix):]), 'w+b')
            file.write(data)
            file.close()

            # read unziped log csv file into pandas dataframe
            data = pandas.read_csv(file.name, dtype=object, low_memory=False)

            # remove records not 'LineItem'
            df = data[(data['RecordType'] == 'LineItem')]

            # check columns for lost tags and add them for the firs two months logs
            columns = list(df.columns.values)
            for tag in self.config.bill_tags:
                if not (tag["tag"] in columns):
                    if tag["tag"] == self.config.lost_tag["key"]:
                        df[tag["tag"]] = df.apply(lambda _: self.config.lost_tag["value"], axis=1)
                        #df.loc[tag["tag"]] = self.config.lost_tag["value"] # this not work
                    else:
                        df[tag["tag"]] = df.apply(lambda _: numpy.nan, axis=1)
                        #df.loc[tag["tag"]] = numpy.nan

            # save to local csv
            csvname = self.config.raw_prefix+filename[len(self.config.log_prefix):]
            csvpath = os.path.join(self.config.data_dir, csvname)
            df.to_csv(csvpath, index=False)

            # upload to s3 processed bucket
            data = open(csvpath, 'rb')
            s3key = self.config.raw_folder+csvname
            file_obj = self.s3_resource.Bucket(self.config.proc_bucket).put_object(Key=s3key, Body=data)

            # remove local csv
            os.remove(csvpath)

        # remove zipfile
        os.remove(zipfile)


def main():
    """
    main function for this script
    :return:
    """

    # get options
    scope, profile, config_yaml = getopts()

    # init config
    config = Config(scope=scope,config_yaml=config_yaml)

    # init AWS_Access instance
    aws = AWS_Access(profile=profile, config=config)

    # get bill log objects list
    log_objects = aws.get_logs()

    # unzip bill log zip file
    for obj in log_objects:
        print obj["Key"]
        aws.upzip_log(obj_key=obj["Key"])




if __name__ == '__main__':
    main()