# -*- coding:utf-8 -*-
#
# Script for aws bill data processing and statistics
# Get options from command line
# Print message to stdout

import getopt
import sys
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

    def usage(self):
        """
        out put command line usage help
        :return:
        """
        print "please usage:"
        print "python " + sys.argv[0] + " [-s scope] + [-p profile] [-c config_yaml]"

        # for option -s
        print ""
        print "    -s: options for processing scope"
        print "         all: bills of all month "
        print "         last: bill of last month"
        print "         latest: bill of latest month"
        print "         yyyy-mm: bill of yyyy-mm month"
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

        # for option -e
        print ""
        print "    -e: running environment"
        print "         local: running local files only "
        print "         s3: download/upload from/to s3"
        print "         default: s3"
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
            opts,args = getopt.getopt(sys.argv[1:],"hs:p:c:e:")
            self.script = sys.argv[0]
            for op, value in opts:
                if op == "-s":
                    self.scope = value
                elif op == "-p":
                    self.profile = value
                elif op == "-c":
                    self.config = value
                elif op == "-e":
                    self.environment = value
                elif op == "-h":
                    self.usage()
                    sys.exit(0)

        except Exception as e:
            """
            process option error
            """
            print ("open exception: %s: %s\n" %(e.args, e.message))
            self.usage()
            sys.exit(1)


        print "\n................\n"
        print "python " + sys.argv[0] + " -s " + self.scope + \
              " -p " + self.profile + " -c " + self.config_yaml + \
              " -e " + self.environment
        print "\n................\n"

        self.msg("I am running, one moment please......\n")
