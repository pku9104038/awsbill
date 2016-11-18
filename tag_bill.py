# -*- coding:utf-8 -*-
#
# Script for aws bill data processing and statistics
# Upload cost_tag files to s3

import boto3
import zipfile as zip
import pandas
import sys
import os
import getopt
import yaml
import numpy
import time

# Define the usage helper and get options function

def usage():
    """
    print usage command help
    :return:
    """
    print "please usage:"
    print "python "+ sys.argv[0] +" [-s scope] + [-p profile] [-c config_yaml] [-o operate] [-f file]"

    # for option -s
    print ""
    print "    -s: options for processing scope"
    print "         all: cost_tags-yyyy-mm.csv for all month "
    print "         last: cost_tags-yyyy-mm.csv for last month"
    print "         latest: cost_tags-yyyy-mm.csv for latest month"
    print "         yyyy-mm: cost_tags-yyyy-mm.csv"
    print "         origin: cost_tags.csv"
    print "         default: origin"
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

    # for option -o
    print ""
    print "    -o: operate mode"
    print "         upload: update cost_tags-yyyy-mm.csv according to [-s scope]"
    print "         download: download cost_tags.csv from s3 to local"
    print "         default: download"
    print ""

    # for option -f
    print ""
    print "    -f: file"
    print "         file used to init/upload to s3 according to [-s scope]"
    print "         default: download"
    print ""

def getopts():
    """
    get options from command line parameters
    :return: scope
    """


    # set defaults
    opts = []
    args = []

    scope = "origin"
    profile = "default"
    config = "config2.yaml"
    operate = "download"
    file = "cost_tags.csv"

    # get options
    try:
        """
        try to get commandline options
        """
        opts,args = getopt.getopt(sys.argv[1:],"h:s:p:c:o:f:")
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
        print "runnning default options: python "+ sys.argv[0] +" -s "+ scope + \
                    " -p "+ profile + " -c " + config + " -o " + operate + " -f " + file
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
        elif op == "-o":
            operate = value
        elif op == "-f":
            file = value
        elif op == "-h":
            usage()
            sys.exit()

    # block all options after firstime running
    # because this might cause history cost been recalculated
    if scope == "all":
        scope = "default"

    return scope, profile, config, operate, file

# Define script scope variables

class Config(object):
    """
    calss for process environments configurations
    """

    def __init__(self, scope, config_yaml, operate):
        # init scope
        self.scope = scope
        self.operate = operate

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
        self.raw_dir = os.path.join(self.cwd, directories["raw"])
        self.tag_dir = os.path.join(self.cwd, directories["tag"])
        self.cal_dir = os.path.join(self.cwd, directories["cal"])
        self.stat_dir = os.path.join(self.cwd, directories["stat"])

        if(not os.path.exists(self.tmp_dir)):
            os.mkdir(self.tmp_dir)
        if (not os.path.exists(self.data_dir)):
            os.mkdir(self.data_dir)
        if (not os.path.exists(self.raw_dir)):
            os.mkdir(self.raw_dir)
        if (not os.path.exists(self.tag_dir)):
            os.mkdir(self.tag_dir)
        if (not os.path.exists(self.cal_dir)):
            os.mkdir(self.cal_dir)
        if (not os.path.exists(self.stat_dir)):
            os.mkdir(self.stat_dir)

        # tags
        self.bill_tags = self.yaml_obj.get("bill_tags")
        self.lost_tag = self.yaml_obj.get("lost_tag")

        # cost tags
        self.cost_tags_file = self.yaml_obj.get("cost_tags")["tags_file"]
        self.cost_start = self.yaml_obj.get("cost_tags")["start_month"]
        self.month_list = []

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

    def download_cost_tag(self):
        """
        download cost tag file from s3 to data dir
        :return:
        """
        s3_key = self.config.tag_folder+self.config.tag_prefix+self.config.cost_tag_file

        # download cost tags file
        obj = self.s3_resource.Object(self.config.log_bucket, s3_key)
        tagfile = os.path.join(self.config.data_dir, obj.key)
        obj.download_file(tagfile)


    def get_cost_tags(self):
        """
        get object list of cost tags file
        return the member according to the scope option

        :param scope: all, latest, last, yyyy-mm
        :return:
        """
        # cost tagss object list
        obj_list = self.s3_client.list_objects(Bucket=self.config.proc_bucket, \
                                               Prefix=self.config.tag_folder + \
                                                      self.config.tag_prefix)["Contents"]

        # fetch item from the list according to the scope option
        objs = []
        scope = self.config.scope
        if (scope == "all"):  # all
            objs = obj_list
        elif (scope == "latest" and len(obj_list) > 1):  # latest
            obj = obj_list[len(obj_list) - 1]
            objs.append(obj)
        elif (scope == "last" and len(obj_list) > 2):  # last
            obj = obj_list[len(obj_list) - 2]
            objs.append(obj)
        else:  # yyyy-mm
            for obj in obj_list:
                filename = obj["Key"]
                month = filename[len(self.config.tag_folder+self.config.tag_prefix):\
                    len(self.config.tag_folder+self.config.tag_prefix) + 7]
                if (month == scope):
                    objs.append(obj)

    def upzip_log(self,obj_key):
        """
        download zipfile of bill logs according to the obj_key
        unzip into csv file
        complement lost user:Tags
        save to csv
        upload to S3

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


    def yyyy_mm(self, year, month):
        """

        :param year:
        :param month:
        :return: yyyy-mm
        """
        if month < 10:
            return str(year) + "-0" + str(month)
        else:
            return str(year) + "-" + str(month)

    def get_months(self):
        """

        :return:
        """

        month_list = []
        start_month = self.config.cost_start
        now = time.localtime(time.time())
        cur_year = now.tm_year
        cur_mon = now.tm_mon
        sta_year = int(start_month[:4])
        sta_mon = int(start_month[5:])
        year = cur_year
        month = cur_year

        if self.config.scope == "all":
            year = sta_year
            month = sta_mon
            end_year = cur_year
            end_mon = cur_mon
            while True:

                month_list.append(self.yyyy_mm(year=year, month=month))

                if year == end_year and month == end_mon:
                    break
                else:
                    if month == 12:
                        month = 1
                        year = year+1
                    else:
                        month = month+1
        elif self.config.scope == "origin":
            year = year
        elif self.config.scope == "last":
            if cur_mon == 1:
                month = 12
                year = cur_year - 1
            else:
                month = cur_mon - 1
                year = cur_year
            month_list.append(self.yyyy_mm(year=year, month=month))
        elif self.config.scope == "latest":
            year = cur_year
            month = cur_mon
            month_list.append(self.yyyy_mm(year=year, month=month))
        else:
            month_list.append(self.config.scope)

        self.config.month_list =  month_list



    def download_tags_file(self):

        None


    def upload_tags_file(self):

        # upload to s3 processed bucket
        csvpath = os.path.join(self.config.tag_dir,self.config.cost_tags_file)
        print "open "+ csvpath
        data = open(csvpath, 'rb')
        if self.config.scope == "origin":
            csvname = self.config.cost_tags_file
            s3key = self.config.tag_folder + csvname
            print "upload origin "+csvname
            file_obj = self.s3_resource.Bucket(self.config.proc_bucket).put_object(Key=s3key, Body=data)
        else:
            for month in self.config.month_list:
                csvname = self.config.cost_tags_file[:len(self.config.cost_tags_file)-4] \
                          + "-" +month+".csv"
                s3key = self.config.tag_folder + csvname
                print "upload " + csvname
                file_obj = self.s3_resource.Bucket(self.config.proc_bucket).put_object(Key=s3key, Body=data)


    def tag_files(self):
        """

        :return:
        """
        if self.config.operate == "download":
            None

        elif self.config.operate == "upload":
            self.upload_tags_file()

        elif self.config.operate == "update":
            None


def main():
    """
    main function for this script
    :return:
    """

    # get options
    scope, profile, config_yaml, operate, file = getopts()

    # init config
    config = Config(scope=scope,config_yaml=config_yaml, operate=operate)

    # init AWS_Access instance
    aws = AWS_Access(profile=profile, config=config)
    print aws.config.scope

    aws.get_months()
    aws.tag_files()

if __name__ == '__main__':
    main()