# -*- coding:utf-8 -*-
#
# Script for aws bill data processing and statistics
# Download raw bill and cost tag csv files
# Join them into cal-bills
# Upload to S3


import config as cfg
import sys, getopt, os
import traceback
import pandas

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
    print "         all: cal-yyyy-mm.csv for all month "
    print "         last: cal-yyyy-mm.csv for last month"
    print "         latest: cal-yyyy-mm.csv for latest month"
    print "         yyyy-mm: cal-yyyy-mm.csv"
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
    print "         default: config2.yaml"
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
        print "############ "
        print "runnning default options: "
        print "python "+ sys.argv[0] +" -s "+ scope + \
                    " -p "+ profile + " -c " + config
        print "############ "
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



class AWS_Access(object):
    """
    class for aws access and bill calculations
    """

    def __init__(self, config):
        """

        :param config:
        """
        self.config = config
        self.session = config.session
        self.s3_client = config.s3_client
        self.s3_resource = config.s3_resource


    def cal_bill(self, month, bill_file, tag_file):
        """

        :param bill_file:
        :param tag_file:
        :return: cal_file, cal_name
        """
        cal_name = self.config.cal_prefix + month + ".csv"
        cal_file = os.path.join(self.config.cal_dir, cal_name)

        # read bill csv file into pandas dataframe
        bill_data = pandas.read_csv(bill_file, dtype=object, low_memory=False)
        #print bill_file

        # read bill csv file into pandas dataframe
        tag_data = pandas.read_csv(tag_file, dtype=object, low_memory=False)
        #print tag_file

        cal_data = pandas.merge(left=bill_data,right=tag_data, how="left", \
                                on=self.config.cost_tags_join_key)
        #print cal_data

        

        df_null = cal_data[(cal_data['user:Project'].isnull())]
        df_totalcost = cal_data['UnBlendedCost']
        df_totalcost = pandas.to_numeric(df_totalcost)
        df_nullcost = df_null['UnBlendedCost']
        df_nullcost = pandas.to_numeric(df_nullcost)
        sum_total = df_totalcost.sum()
        sum_null = df_nullcost.sum()
        if sum_null == 0:
            null_rate = 1
        else:
            null_rate = sum_total / (sum_total - sum_null)
            cal_data['NullRate'] = cal_data.apply(lambda _: null_rate, axis=1)

        cal_data.to_csv(cal_file, index=False)

        return cal_file, cal_name

    def cal_bills(self, month_list = []):
        """

        :param month_list:
        :return:
        """

        if len(month_list) > 0:
            for month in month_list:
                try:
                    """
                    try download bill and tag files, then cal and upload
                    """
                    # download bill file
                    bill_file = self.config.raw_prefix + month + ".csv"
                    bill_key = self.config.raw_folder + bill_file
                    print "download " +bill_key
                    obj = self.s3_resource.Object(self.config.proc_bucket, bill_key)
                    bill_file = os.path.join(self.config.raw_dir, bill_file)
                    obj.download_file(bill_file)

                    # download bill file
                    tag_file = self.config.tag_prefix + month + ".csv"
                    tag_key = self.config.tag_folder + tag_file
                    print "download " + tag_key
                    obj = self.s3_resource.Object(self.config.proc_bucket, tag_key)
                    tag_file = os.path.join(self.config.tag_dir, tag_file)
                    obj.download_file(tag_file)

                    # cal bill of this month
                    print "cal " + month
                    cal_file, cal_name = self.cal_bill( month=month, \
                                                        bill_file=bill_file, \
                                                        tag_file = tag_file)

                    # upload cal bill to s3 processed bucket
                    data = open(cal_file, 'rb')
                    s3key = self.config.cal_folder + cal_name
                    print "upload " + cal_name
                    file_obj = self.s3_resource.Bucket( \
                        self.config.proc_bucket).put_object(Key=s3key, Body=data )

                except Exception as e:
                    """
                    process option error
                    """
                    #print ("open exception: %s: %s\n" % (e.args, e.message))
                    traceback.print_exc()



def main():
    """
    main function for this script
    :return:
    """

    # get options
    scope, profile, config_yaml= getopts()

    # init Config instance
    config = cfg.Config(scope=scope,config_yaml=config_yaml, profile=profile)

    print config.month_list

    # init AWS_Access instance
    aws = AWS_Access(config=config)
    aws.cal_bills(month_list=config.month_list)


if __name__ == '__main__':
    main()