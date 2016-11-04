# -*- coding:utf-8 -*-
#
# Script for aws bill data processing and statistics
# Download cal bill and merge into stat_bill
# Upload to S3

import config as cfg
import sys, os, getopt
import traceback
import pandas

# Define the usage helper and get options function

def usage():
    """
    print usage command help
    :return:
    """
    print "please usage:"
    print "python "+ sys.argv[0] +" [-o operate] + [-p profile] [-c config_yaml]"

    # for option -o
    print ""
    print "    -o: options for statistics operations"
    print "         merge: merge all cal-yyyy-mm.cdv into history-bill.csv "
    print "         default: merge"
    print ""

    # for option -s
    print ""
    print "    -s: options for processing scope"
    print "         all: cal-yyyy-mm.csv for all month "
    print "         default: all"
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
    :return: operate, profile, config, scope
    """


    # set defaults
    opts = []
    args = []

    scope = "all"
    operate = "merge"
    profile = "default"
    config = "config2.yaml"


    # get options
    try:
        """
        try to get commandline options
        """
        opts,args = getopt.getopt(sys.argv[1:],"h:o:p:c:s:")
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
        print "python "+ sys.argv[0] +" -o "+ operate + \
                    " -p "+ profile + " -c " + config + " -s " + scope
        print "############ "
        # continue script with default options

    # set options according to command line input
    for op, value in opts:
        if op == "-o":
            operate = value
        elif op == "-p":
            profile = value
        elif op == "-c":
            config = value
        elif op == "-s":
            scope = value
        elif op == "-h":
            usage()
            sys.exit()

    return operate, profile, config, scope


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


    def merge_bills(self, month_list = []):
        """

        :param month_list:
        :return:
        """


        dataframe = None
        count = False
        if len(month_list) > 0:
            for month in month_list:
                try:
                    """
                    try download bill and tag files, then cal and upload
                    """
                    # download cal_bill file
                    bill_file = self.config.cal_prefix + month + ".csv"
                    bill_key = self.config.cal_folder + bill_file
                    print "download " +bill_key
                    obj = self.s3_resource.Object(self.config.proc_bucket, bill_key)
                    bill_file = os.path.join(self.config.cal_dir, bill_file)
                    obj.download_file(bill_file)

                    # read bill csv file into pandas dataframe
                    bill_data = pandas.read_csv(bill_file, dtype=object, low_memory=False)

                    print "merge " + month
                    if not count:
                        dataframe = bill_data.copy()
                        count = True
                    else:
                        data = [dataframe, bill_data]
                        dataframe = pandas.concat(data)


                except Exception as e:
                    """
                    process option error
                    """
                    #print ("open exception: %s: %s\n" % (e.args, e.message))
                    traceback.print_exc()

        # upload merged bill to s3 processed bucket
        merge_path = os.path.join(self.config.stat_dir,\
                                      self.config.merge_file)

        dataframe.to_csv(merge_path,index=False)

        data = open(merge_path, 'rb')
        s3key = self.config.stat_folder + self.config.merge_file
        print "upload " + self.config.merge_file
        file_obj = self.s3_resource.Bucket( \
                    self.config.proc_bucket).put_object(Key=s3key, Body=data)


def main():
    """
    main function for this script
    :return:
    """

    # get options
    operate, profile, config_yaml, scope= getopts()

    # init Config instance
    config = cfg.Config(scope=scope,config_yaml=config_yaml,\
                        profile=profile, operate=operate)

    print config.month_list

    # init AWS_Access instance
    aws = AWS_Access(config=config)
    aws.merge_bills(month_list=config.month_list)


if __name__ == '__main__':
    main()