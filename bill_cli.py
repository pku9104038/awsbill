# -*- coding:utf-8 -*-
#
# Script for aws bill data processing and statistics
# Get options from command line
# Print message to stdout

import getopt
import sys, time
import datetime

class CommandLine(object):
    """
    class for command line operation and stdout
    """

    def __init__(self):
        self.scope = "latest"
        self.profile = "default"
        self.config_yaml = "config2.yaml"
        self.environment = "s3"
        self.script = ""
        now = time.localtime(time.time())
        self.end_month = ""

    def usage(self):
        """
        out put command line usage help
        :return:
        """
        print "please usage:"
        print "python " + sys.argv[0] + "[-p profile] [-c config_yaml] " + \
              "[-l data_location] " + " [-m month] + [-e end_month] "


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

        # for option -l
        print ""
        print "    -l: file location"
        print "         local: running local files only "
        print "         s3: download/upload from/to s3"
        print "         default: s3"
        print ""

        # for option -m
        print ""
        print "    -m: options for processing months"
        print "         all: bills of all month "
        print "         last: bill of last month"
        print "         latest: bill of latest month"
        print "         yyyy-mm: bill of yyyy-mm month"
        print "         default: latest"
        print ""

        # for option -e
        print ""
        print "    -e: options for processing end month"
        print "        work with -m yyyy-mm only"
        print "         yyyy-mm: bill of yyyy-mm month"
        print "         default: = -m option"
        print ""

    def now(self):
        """

        :return:
        """
        return datetime.datetime.strftime(datetime.datetime.now(), \
                                          '%Y-%m-%d %H:%M:%S')

    def msg(self,message):
        print self.now() + " ...... " + message

    def get_options(self):
        """
        get options from command line
        :return:
        """
        # set defaults
        # opts = []
        # args = []

        # get options
        try:
            """
            try to get commandline options
            """
            opts,args = getopt.getopt(sys.argv[1:],"hm:e:p:c:l:")
            self.script = sys.argv[0]
            for op, value in opts:
                if op == "-m":
                    self.scope = value
                if op == "-e":
                    self.end_month = value
                elif op == "-p":
                    self.profile = value
                elif op == "-c":
                    self.config = value
                elif op == "-l":
                    self.environment = value
                elif op == "-h":
                    self.usage()
                    sys.exit(0)

            if self.scope == "all" or self.scope == "latest" or \
                            self.scope == "last" or self.end_month == "":
                self.end_month = self.scope



        except Exception as e:
            """
            process option error
            """
            print ("open exception: %s: %s\n" %(e.args, e.message))
            self.usage()
            sys.exit(1)


        print "\n................\n"
        print "python " + sys.argv[0] + " -p " + self.profile + \
              " -c " + self.config_yaml + " -l " + self.environment +\
              " -m " + self.scope + " -e " +self.end_month
        print "\n................\n"

        self.msg("I am running, one moment please......\n")
