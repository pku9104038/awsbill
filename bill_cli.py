# -*- coding:utf-8 -*-
#
# Script for aws bill data processing and statistics
# Get options from command line
# Print message to stdout

import getopt
import sys
import datetime


class Options(object):
    """

    """
    def __init__(self):
        self.scope = "latest"
        self.profile = "default"
        self.config_yaml = "config2.yaml"
        self.environment = "s3"
        self.end_month = ""
        self.remove = "no"




class CommandLine(object):
    """
    class for command line operation and stdout
    """

    def msg(self, message):
        print self.now() + " ...... " + message

    def __init__(self):
        self.option = Options()
        self.script = ""

        """
        self.scope = "latest"
        self.profile = "default"
        self.config_yaml = "config2.yaml"
        self.environment = "s3"

        now = time.localtime(time.time())
        self.end_month = ""


        """

        self.msg("I am here, one moment please......\n")

    def usage(self):
        """
        out put command line usage help
        :return:
        """
        print "please usage:"
        print "python " + sys.argv[0] + "[-p profile] [-c config_yaml] " + \
              "[-l data_location] " + " [-m month] + [-e end_month] " + \
              "[-r y/n] "


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

        # for option -r
        print ""
        print "    -r: remove local file"
        print "         yes: remove"
        print "         no: keep"
        print "         default: no"
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
            opts,args = getopt.getopt(sys.argv[1:],"hm:e:p:c:l:r:")
            self.script = sys.argv[0]
            for op, value in opts:
                if op == "-m":
                    self.option.scope = value
                if op == "-e":
                    self.option.end_month = value
                elif op == "-p":
                    self.option.profile = value
                elif op == "-c":
                    self.option.config = value
                elif op == "-l":
                    self.option.environment = value
                elif op == "-r":
                    self.option.remove = value
                elif op == "-h":
                    self.usage()
                    sys.exit(0)

            if self.option.scope == "all" or self.option.scope == "latest" or \
                            self.option.scope == "last" or self.option.end_month == "":
                self.option.end_month = self.option.scope



        except Exception as e:
            """
            process option error
            """
            print ("open exception: %s: %s\n" %(e.args, e.message))
            self.usage()
            sys.exit(1)


        print "\n................\n"
        print "python " + sys.argv[0] + " -p " + self.option.profile + \
              " -c " + self.option.config_yaml + " -l " + self.option.environment +\
              " -m " + self.option.scope + " -e " + self.option.end_month +\
              " -r " + self.option.remove
        print "\n................\n"



    def set_options(self,scope=None,profile=None,config_yaml=None, \
                    environment = None, remove= None, end_month= None):
        """

        :param scope:
        :param profile:
        :param config_yaml:
        :param environment:
        :param remove:
        :param end_month:
        :return:
        """

        if scope != None:
            self.scope =scope

        if profile != None:
            self.profile = profile

        if config_yaml != None:
            self.config_yaml = config_yaml

        if environment != None:
            self.environemnt = environment

        if remove != None:
            self.remove = remove

        if end_month != None:
            self.end_month = end_month