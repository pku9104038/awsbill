# -*- coding:utf-8 -*-
#
# Script for aws bill data processing and statistics
# Download raw bill and cost tag csv files
# Join them into cal-bills
# Upload to S3


import config as cfg
import bill_cli
import sys
import getopt
import os
import shutil
import datetime
import traceback
import pandas, numpy
import time

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


    # for option -e
    print ""
    print "    -e: running environment"
    print "         local: running local files only "
    print "         s3: download/upload from/to s3"
    print "         default: s3"
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
    environment = "local"

    # get options
    try:
        """
        try to get commandline options
        """
        opts,args = getopt.getopt(sys.argv[1:],"h:s:p:c:e:")
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
                    " -p "+ profile + " -c " + config + " -e " + environment
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
        elif op == "-e":
            environment = value
        elif op == "-h":
            usage()
            sys.exit()

    return scope, profile, config, environment


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


    def set_index(self,row):
        """

        :param row:
        :return:
        """
        return str(row["RecordId"]) + "-" + str(row["SubscriptionId"]) + "-" +\
                str(row["RateId"]) +"-" + str(row["LinkedAccountId"]) + "-" + \
               str(row["InvoiceID"])


    def set_index_of_records(self,data):
        """

        :param data:
        :return:
        """

        null_recordid = data[data.RecordId.isnull()]
        index = null_recordid.index
        for i in index:
            data["RecordId"][i] = data["RateId"][i] # use rate id as record id

        nul_subscription = data[data.SubscriptionId.isnull()]
        index = nul_subscription.index
        for i in index:
            data["SubscriptionId"][i] = data["RateId"][i]  # use rate id as record id

        data["index"] = data.apply(self.set_index, axis=1)


    def get_instance_type(self,row):
        """

        :param row:
        :return:
        """

        product = str(row["ProductName"])
        usage_type = str(row["UsageType"])
        desc = str(row["ItemDescription"])

        # DynamoDB
        if product == "Amazon DynamoDB":
            if usage_type.find("write") > -1 or usage_type.find("Write") > -1:
                return "WriteCapacity"
            elif usage_type.find("read") > -1 or usage_type.find("Read") > -1:
                return "ReadCapacity"
            elif usage_type.find("TimedStorage") > -1 :
                return "TimedStorage"
            else:
                return None


        # ElastiCache
        elif product == "Amazon ElastiCache":
            colon = usage_type.find(":")
            if colon > -1:
                return usage_type[colon + len(":"):]
            else:
                return None

        # RedShift
        elif product == "Amazon Redshift":
            colon = usage_type.find(":")
            if colon > -1:
                return usage_type[colon + len(":"):]
            else:
                return None

        # S3
        elif product == "Amazon Simple Storage Service":
            if usage_type.find("DataTransfer") > -1:
                return "DataTransfer"
            elif usage_type.find("EarlyDelete") > -1:
                return "EarlyDelete"
            elif usage_type.find("Requests") > -1:
                return "Requests"
            elif usage_type.find("TimedStorage") > -1:
                return "TimedStorage"
            else:
                return None

        # SQS
        elif product == "Amazon Simple Queue Service":
            if usage_type.find("Requests") > -1:
                return "Requests"
            else:
                return None


        # SNS
        elif product == "Amazon Simple Notification Service":
            if usage_type.find("DataTransfer") > -1:
                return "DataTransfer"
            elif usage_type.find("DeliveryAttempts") > -1:
                return "DeliveryAttempts"
            elif usage_type.find("Requests") > -1:
                return "Requests"
            else:
                return None

        # CloudWatch
        elif product == "AmazonCloudWatch":
            if usage_type.find("DataProcessing") > -1:
                return "DataProcessing"
            return None

        # CloudTrial
        elif product == "AWS CloudTrail":
            if usage_type.find("EventsRecorded") > -1:
                return "EventsRecorded"
            return None

        # RDS
        elif product == "Amazon RDS Service":
            colon = usage_type.find(":")
            if colon > -1:
                return usage_type[colon + len(":"):]
            else:
                if usage_type.find("DataTransfer") > -1:
                    return "DataTransfer"
                elif usage_type.find("CloudFront") > -1:
                    return "CloudFront"
                return None


        # EC2
        elif product == "Amazon Elastic Compute Cloud":
            colon = usage_type.find(":")
            if colon > -1:
                return usage_type[colon + len(":"):]
            else:
                if usage_type.find("LoadBalancer") > -1:
                    return "LoadBalancer"
                elif usage_type.find("DataTransfer") > -1:
                    return "DataTransfer"
                elif usage_type.find("DataProcessing") > -1:
                    return "DataProcessing"
                elif usage_type.find("CloudFront") > -1:
                    return "CloudFront"
                return None

        # Support
        elif product.find("AWS Support") > -1:
            if product.find("Developer") > -1:
                return "Developer"
            elif product.find("Business") > -1:
                return "Business"
            return None


                # NULL: VAT
        else:
            if desc.find("VAT") > -1:
                return "VAT"
            else:
                return None


    def get_platform(self, row):
        """

        :param row:
        :return:
        """

        product = str(row["ProductName"])
        desc = str(row["ItemDescription"])
        usage_type = str(row["UsageType"])

        # DynamoDB
        if product == "Amazon DynamoDB":
                return "DynamoDB"


        # ElastiCache
        elif product == "Amazon ElastiCache":
            if desc.find("Memcached") > -1:
                return "Memcached"
            elif desc.find("Redis") > -1:
                return "Redis"
            else:
                return "ElastiCache"

        # RedShift
        elif product == "Amazon Redshift":
            return "Redshift"

        # S3
        elif product == "Amazon Simple Storage Service":
            if desc.find("Glacier") > -1:
                return "Glacier"
            else:
                return "S3"

        # SQS
        elif product == "Amazon Simple Queue Service":
            return "SQS"

        # SNS
        elif product == "Amazon Simple Notification Service":
            return "SNS"


        # CloudWatch
        elif product == "AmazonCloudWatch":
            return "CloudWatch"

        # CloudTrial
        elif product == "AWS CloudTrail":
            return "CloudTrail"

        # RDS
        elif product == "Amazon RDS Service":
            if desc.find("MySQL") > -1:
                return "MySQL"
            elif desc.find("SQL Server") > -1:
                return "SQL Server"
            elif desc.find("PostgreSQL") > -1:
                return "PostgreSQL"
            elif desc.find("Oracle") > -1:
                return "Oracle"
            elif desc.find("Oracle") > -1:
                return "Oracle"
            else:
                return "RDS"


        # EC2
        elif product == "Amazon Elastic Compute Cloud":
            if desc.find("Linux") > -1:
                return "Linux"
            elif desc.find("Windows") > -1:
                return "Windows"
            elif desc.find("LoadBalancer") > -1:
                return "ELB"
            elif desc.find("Elastic IP") > -1:
                return "EIP"
            elif desc.find("Elastic IP") > -1:
                return "EIP"
            elif desc.find("CloudFront") > -1:
                return "CloudFront"
            else:
                if usage_type.find("EBSOptimized") > -1:
                    return "EBSOptimized"
                elif usage_type.find("EBS") > -1:
                    return "EBS"
                elif usage_type.find("CW") > -1:
                    return "CloudWatch"
                return "EC2"

        # Support
        elif product.find("AWS Support") > -1:
            return "Support"




            # NULL: VAT
        else:
            if desc.find("VAT") > -1:
                return "VAT"
            else:
                return None


    def set_platform_instance_type(self,data):
        """

        :param row:
        :return:
        """

        platform = None
        instance_type = None
        data.loc[:,"InstanceType"] = instance_type
        data.loc[:, "Platform"] = platform

        grouped = data.groupby(["ProductName","UsageType","ItemDescription"])

        for name, group in grouped:
            product = name[0]
            usage_type = name[1]
            desc = name[2]

            # DynamoDB
            if product == "Amazon DynamoDB":
                platform = "DynamoDB"

                if usage_type.find("write") > -1 or usage_type.find("Write") > -1:
                    instance_type =  "WriteCapacity"
                elif usage_type.find("read") > -1 or usage_type.find("Read") > -1:
                    instance_type =  "ReadCapacity"
                elif usage_type.find("TimedStorage") > -1:
                    instance_type = "TimedStorage"



            # ElastiCache
            elif product == "Amazon ElastiCache":
                colon = usage_type.find(":")
                if colon > -1:
                    instance_type = usage_type[colon + len(":"):]

                if desc.find("Memcached") > -1:
                    #platform = "Memcached"
                    platform = "ElastiCache"   # ElastiCache RI could show engine info
                elif desc.find("Redis") > -1:
                    #platform = "Redis"
                    platform = "ElastiCache"
                else:
                    platform = "ElastiCache"

            # RedShift
            elif product == "Amazon Redshift":
                colon = usage_type.find(":")
                if colon > -1:
                    instance_type = usage_type[colon + len(":"):]
                platform = "Redshift"

            # S3
            elif product == "Amazon Simple Storage Service":
                if usage_type.find("DataTransfer") > -1:
                    instance_type = "DataTransfer"
                elif usage_type.find("EarlyDelete") > -1:
                    instance_type = "EarlyDelete"
                elif usage_type.find("Requests") > -1:
                    instance_type = "Requests"
                elif usage_type.find("TimedStorage") > -1:
                    instance_type = "TimedStorage"


                if desc.find("Glacier") > -1:
                    platform = "Glacier"
                else:
                    platform = "S3"


            # SQS
            elif product == "Amazon Simple Queue Service":
                if usage_type.find("Requests") > -1:
                    instance_type =  "Requests"
                platform = "SQS"


            # SNS
            elif product == "Amazon Simple Notification Service":
                if usage_type.find("DataTransfer") > -1:
                    instance_type = "DataTransfer"
                elif usage_type.find("DeliveryAttempts") > -1:
                    instance_type = "DeliveryAttempts"
                elif usage_type.find("Requests") > -1:
                    instance_type = "Requests"
                platform = "SNS"

            # CloudWatch
            elif product == "AmazonCloudWatch":
                if usage_type.find("DataProcessing") > -1:
                    instance_type = "DataProcessing"
                platform = "CloudWatch"

            # CloudTrial
            elif product == "AWS CloudTrail":
                if usage_type.find("EventsRecorded") > -1:
                    instance_type = "EventsRecorded"
                platform = "CloudTrail"

            # RDS
            elif product == "Amazon RDS Service":
                colon = usage_type.find(":")
                if colon > -1:
                    instance_type = usage_type[colon + len(":"):]
                else:
                    if usage_type.find("DataTransfer") > -1:
                        instance_type = "DataTransfer"
                    elif usage_type.find("CloudFront") > -1:
                        instance_type = "CloudFront"

                if desc.find("MySQL") > -1:
                    platform = "MySQL"
                elif desc.find("SQL Server") > -1:
                    platform = "SQL Server"
                elif desc.find("PostgreSQL") > -1:
                    platform = "PostgreSQL"
                elif desc.find("Oracle") > -1:
                    platform = "Oracle"
                elif desc.find("Oracle") > -1:
                    platform = "Oracle"
                else:
                    platform = "RDS"

            # EC2
            elif product == "Amazon Elastic Compute Cloud":
                colon = usage_type.find(":")
                if colon > -1:
                    instance_type = usage_type[colon + len(":"):]
                else:
                    if usage_type.find("LoadBalancer") > -1:
                        instance_type = "LoadBalancer"
                    elif usage_type.find("DataTransfer") > -1:
                        instance_type = "DataTransfer"
                    elif usage_type.find("DataProcessing") > -1:
                        instance_type = "DataProcessing"
                    elif usage_type.find("CloudFront") > -1:
                        instance_type = "CloudFront"

                if desc.find("Linux") > -1:
                    platform = "Linux"
                elif desc.find("RHEL") > -1:
                    platform = "RHEL"
                elif desc.find("Windows") > -1:
                    platform = "Windows"
                elif desc.find("SQL Std") > -1:
                    platform = "SQL Std"
                elif desc.find("LoadBalancer") > -1:
                    platform = "ELB"
                elif desc.find("Elastic IP") > -1:
                    platform = "EIP"
                elif desc.find("Elastic IP") > -1:
                    platform = "EIP"
                elif desc.find("CloudFront") > -1:
                    platform = "CloudFront"
                else:
                    if usage_type.find("EBSOptimized") > -1:
                        platform = "EBSOptimized"
                    elif usage_type.find("EBS") > -1:
                        platform = "EBS"
                    elif usage_type.find("CW") > -1:
                        platform = "CloudWatch"
                    else:
                        platform = "EC2"

            # set InstanceType , Platform
            index = group.index
            data["InstanceType"][index] = instance_type
            data["Platform"][index] = platform


        # with null items in ProductName, UsageType, ItemDescription
        # can not been grouped
        df = data[data["ItemDescription"]=="税金 VAT 类型"]
        index = df.index
        data["ProductName"][index] = "VAT"
        data["InstanceType"][index] = "VAT"
        data["Platform"][index] = "VAT"
        data["UsageType"][index] = "VAT"

        df = data[data["ItemDescription"] == "Recurring Fee"]
        index = df.index
        data["InstanceType"][index] = "Support"
        data["Platform"][index] = "Support"
        data["UsageType"][index] = "Support"


    def tag_metric_monitor_usage(self, data):
        """

        :param data:
        :return:
        """

        project = None
        grouped = data.groupby(["ProductName","UsageType","ResourceId"])
        for name,group in grouped:
            if name[0] == "Amazon Elastic Compute Cloud" and \
                name[1].find("MetricMonitorUsage") > -1:
                instance = name[2].find("instance/")
                if instance > 0:
                    instance_id = name[2][instance + len("instance/"):]
                    instance_data = data[data["ResourceId"] == instance_id]
                    project_data = instance_data[~(instance_data["user:Project"].isnull())]
                if len(project_data.index) > 0:
                        project = project_data["user:Project"][project_data.first_valid_index()]
                        index = group.index
                        data["user:Project"][index] = project


    def tag_by_resourceid_group(self,data):
        """

        :param data:
        :return:
        """

        grouped = data.groupby("ResourceId")

        for name, group in grouped:
            resourceid = name[0]
            bull_data = group[(group["user:Project"].isnull())]
            index = bull_data.index
            if len(index) > 0:
                project_data = group[~(group["user:Project"].isnull())]
                if len(project_data.index) > 0:
                    project = project_data["user:Project"][project_data.first_valid_index()]
                    data["user:Project"][index] = project


    def set_dynamodb_project(self, row):
        """

        :param row:
        :return:
        """

        if row["ProductName"] == "Amazon DynamoDB":
            return self.config.lost_tag["value"]
        else:
            return row["user:Project"]


    def default_adjust_cost(self, row):
        """

        :return:
        """
        return float(row["UnBlendedCost"])


    def adjust_ri_cost(self, data):
        """

        :param data:
        :return:
        """

        grouped = data.groupby(["ProductName","Platform","InstanceType"])

        for name, group in grouped:
            product = name[0]

            # RDS
            if product == "Amazon RDS Service":
                # check ri purchase order
                ripo = group[(group.ResourceId.isnull()) & (group.ReservedInstance == "Y")]
                usage = group[~(group.ResourceId.isnull()) | (group.ReservedInstance == "N")]
                ri_usage = usage[usage["ReservedInstance"] == "Y"]

                # if ri purchaseed
                if len(ripo.index) > 0:
                    # total cost
                    cost = group["UnBlendedCost"].sum()
                    ri_cost = ripo["UnBlendedCost"].sum()
                    usage_cost = usage["UnBlendedCost"].sum()

                    # usage hours
                    hours = usage["UsageQuantity"].sum()
                    # ri hours
                    ri_hours = ripo["UsageQuantity"].sum()
                    # ri hours
                    ri_usage_hours = ri_usage["UsageQuantity"].sum()

                    # average cost
                    if hours > 0:
                        if ri_hours > ri_usage_hours: # not all ri used
                            rate = ((ri_usage_hours/ri_hours) * ri_cost + usage_cost) / hours
                            null_rate = (ri_hours - ri_usage_hours) / ri_hours
                        else:
                            rate = cost / hours
                            null_rate = 0

                        # set AdjustedCost according to index
                        ripo_index = ripo.index
                        usage_index = usage.index
                        (data["AdjustedCost"])[usage_index] = rate
                        for idx in ripo_index:
                            data["AdjustedCost"][idx] = data["UnBlendedCost"][idx] * null_rate

            # EC2
            elif product == "Amazon Elastic Compute Cloud":
                # check ri purchase order
                ripo = group[(group.ResourceId.isnull()) & (group.ReservedInstance == "Y")]
                usage = group[~(group.ResourceId.isnull()) | (group.ReservedInstance == "N")]

                # if ri purchaseed
                if len(ripo.index) > 0:
                    # used cost
                    usage_cost = usage["UnBlendedCost"].sum()

                    # usage hours
                    hours = usage["UsageQuantity"].sum()

                    # average cost
                    if hours > 0:
                        rate = usage_cost / hours
                        # set AdjustedCost according to index
                        usage_index = usage.index
                        (data["AdjustedCost"])[usage_index] = rate

            # ElastiCache
            elif product == "Amazon ElastiCache":

                # RI with up front payment
                # this sign up payment record could not tag to any project
                # keep it as null and share cost by all projects

                # check ri purchase order
                ripo = group[(group.ResourceId.isnull()) & (group.ReservedInstance == "Y")]
                usage = group[~(group.ResourceId.isnull()) | (group.ReservedInstance == "N")]
                ri_usage = usage[usage["ReservedInstance"] == "Y"]

                # if ri purchaseed
                if len(ripo.index) > 0:
                    # total cost
                    cost = group["UnBlendedCost"].sum()
                    ri_cost = ripo["UnBlendedCost"].sum()
                    usage_cost = usage["UnBlendedCost"].sum()

                    # usage hours
                    hours = usage["UsageQuantity"].sum()
                    # ri hours
                    ri_hours = ripo["UsageQuantity"].sum()
                    # ri hours
                    ri_usage_hours = ri_usage["UsageQuantity"].sum()

                    # average cost
                    if hours > 0:
                        if ri_hours > ri_usage_hours:  # not all ri used
                            rate = ((ri_usage_hours / ri_hours) * ri_cost + usage_cost) / hours
                            null_rate = (ri_hours - ri_usage_hours) / ri_hours
                        else:
                            rate = cost / hours
                            null_rate = 0

                        # set AdjustedCost according to index
                        ripo_index = ripo.index
                        usage_index = usage.index
                        (data["AdjustedCost"])[usage_index] = rate
                        for idx in ripo_index:
                            data["AdjustedCost"][idx] = data["UnBlendedCost"][idx] * null_rate


    def ri_analysis(self, data):
        """

        :param data:
        :return:
        """

        # RDS
        product = "Amazon RDS Service"

        # get rds data
        adjust_data = data[data["ProductName"] == product]

        # groupby InstanceType, Platform
        grouped = adjust_data.groupby(["InstanceType", "Platform"])

        for name, group in grouped:

            # check ri purchase order
            ripo = group[(group.ResourceId.isnull()) & (group.ReservedInstance == "Y")]
            usage = group[~(group.ResourceId.isnull()) | (group.ReservedInstance == "N")]
            ri_usage = usage[usage["ReservedInstance"] == "Y"]

            # if ri purchaseed
            if len(ripo.index) > 0:
                # total cost
                cost = group["UnBlendedCost"].sum()
                ri_cost = ripo["UnBlendedCost"].sum()
                usage_cost = usage["UnBlendedCost"].sum()

                # usage hours
                hours = usage["UsageQuantity"].sum()
                # ri hours
                ri_hours = ripo["UsageQuantity"].sum()
                # ri hours
                ri_usage_hours = ri_usage["UsageQuantity"].sum()

                # average cost
                riusage = False
                rate = 0
                null_rate = 1
                if hours > 0:
                    riusage = True
                    if ri_hours <= hours:
                        rate = cost / hours
                        null_rate = 0
                    else:
                        rate = ri_cost * ri_usage_hours / ri_hours / hours \
                               + usage_cost / hours
                        null_rate = (ri_hours - ri_usage_hours) / ri_hours

                data["AdjustedCost"] = data.apply(self.set_rds_adjust_cost, \
                                                  args=(product, \
                                                        name[0], \
                                                        name[1], \
                                                        rate, \
                                                        null_rate, \
                                                        riusage), \
                                                  axis=1)


    def now(self):
        """

        :return:
        """

        return datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')


    def get_bill_date(self,data):
        """

        :param data:
        :return:
        """
        """
        ondemand = data[(data.ItemDescription != "Recurring Fee") \
                           & (data.ItemDescription != "税金 VAT 类型") \
                        & (data.ReservedInstance == "N")]

        VAT = data[data.ItemDescription == "税金 VAT 类型" ]
        usage_end_date = ondemand["UsageEndDate"].max()
        bill_datetime = datetime.datetime.strptime(usage_end_date, \
                                                   "%Y-%m-%d %H:%M:%S")
        vat_end_date = VAT["UsageEndDate"].max()
        end_of_month = datetime.datetime.strptime(vat_end_date, \
                                                  "%Y-%m-%d %H:%M:%S")
        left_datetime = end_of_month - bill_datetime
        hours = left_datetime.seconds/60/60 + left_datetime.days*24
        print "bill time " + usage_end_date
        print str(hours) + " hours left"

        """

        this_month = data[data.Platform != "Support"]
        latest_start = this_month["UsageStartDate"].max()
        start_datetime = datetime.datetime.strptime(latest_start, \
                                                   "%Y-%m-%d %H:%M:%S")
        onehour = datetime.timedelta(hours=1)
        end_datetime = start_datetime + onehour


        VAT = data[data.ItemDescription == "税金 VAT 类型"]
        vat_end_date = VAT["UsageEndDate"].max()
        end_of_month = datetime.datetime.strptime(vat_end_date, \
                                                  "%Y-%m-%d %H:%M:%S")
        left_datetime = end_of_month - end_datetime
        hours = left_datetime.seconds / 60 / 60 + left_datetime.days * 24
        print "bill time " + str(end_datetime)
        print "this month [ "+ str(hours) + " ] hours left"


    def startstamp(self, row):

        try:
            d = datetime.datetime.strptime(row["UsageStartDate"], "%Y-%m-%d %H:%M:%S.%f")
            t = d.timetuple()
            timeStamp = int(time.mktime(t))
            timeStamp = float(str(timeStamp) + str("%06d" % d.microsecond)) / 1000000
            #print timeStamp
            return timeStamp
        except ValueError as e:
            #print e
            d = datetime.datetime.strptime(row["UsageStartDate"], "%Y-%m-%d %H:%M:%S")
            t = d.timetuple()
            timeStamp = int(time.mktime(t))
            timeStamp = float(str(timeStamp) + str("%06d" % d.microsecond)) / 1000000
            #print timeStamp
            return timeStamp


    def endstamp(self, row):

        try:
            d = datetime.datetime.strptime(row["UsageEndDate"], "%Y-%m-%d %H:%M:%S.%f")
            t = d.timetuple()
            timeStamp = int(time.mktime(t))
            timeStamp = float(str(timeStamp) + str("%06d" % d.microsecond)) / 1000000
            # print timeStamp
            return timeStamp
        except ValueError as e:
            # print e
            d = datetime.datetime.strptime(row["UsageEndDate"], "%Y-%m-%d %H:%M:%S")
            t = d.timetuple()
            timeStamp = int(time.mktime(t))
            timeStamp = float(str(timeStamp) + str("%06d" % d.microsecond)) / 1000000
            # print timeStamp
            return timeStamp


    def trim_project_tag(self,data):
        """

        :param data:
        :return:
        """


    def cal_bill(self, month, bill_file, tag_file):
        """

        :param bill_file:
        :param tag_file:
        :return: cal_file, cal_name
        """

        # read bill csv file into pandas dataframe
        print "read bill raw csv: " + month +"......" + self.now()

        bill_data = pandas.read_csv(bill_file, dtype=self.config.calc_read_csv_dtype, low_memory=False)


        # tag metric_monitor_usage according to the instance_id
        print "tag ec2 metric monitor ......" + self.now()
        pad = True
        #bill_data["user:Project"] = bill_data.apply(self.tag_metric_monitor_usage, \
        #                                            args = (bill_data, True), axis=1)
        self.tag_metric_monitor_usage(data = bill_data)

        # set dynamodb lost tag to ztjy only
        print "tag dynamodb ......"+ self.now()
        #bill_data["user:Project"] = bill_data.apply(self.set_dynamodb_project, axis=1)
        dynamo = bill_data[bill_data["ProductName"]=="Amazon DynamoDB"]
        idx = dynamo.index
        bill_data["user:Project"][idx] = self.config.lost_tag["value"]

        # fill null project if this resourceid have been taged this month
        print "tag by resourceid ......" + self.now()
        self.tag_by_resourceid_group(bill_data)

        # read bill csv file into pandas dataframe
        print "read tag csv ..."+ self.now()
        tag_data = pandas.read_csv(tag_file, low_memory=False)

        # left join merge bill and tags
        print "merge bill ......"+ self.now()
        #bill_data.apply(self.trim_project_tag,bill_data)
        cal_data = pandas.merge(left=bill_data,right=tag_data, how="left", \
                                on=self.config.cost_tags_join_key)

        """
        # test code to find some merge error
        # and found that it was caused by tag with white space

        cal_data = bill_data
        cal_data.loc[:,"ProjectGroup"] = None
        cal_data.loc[:, "CostDivision"] = None
        """
        """
        grouped = tag_data.groupby("user:Project")

        for name, group in grouped:
            tag = name
            index = (bill_data[bill_data["user:Project"]==tag]).index
            print (tag, len(index) )
            if len(index) > 0:
                cal_data["ProjectGroup"][index] = group["ProjectGroup"][group.first_valid_index()]
                cal_data["CostDivision"][index] = group["CostDivision"][group.first_valid_index()]
        """
        """
        grouped = cal_data.groupby("user:Project")

        for name, group in grouped:
            tag = name
            index = (bill_data[bill_data["user:Project"] == tag]).index
            print (tag, len(index))
            if len(index) > 0:
                cal_data["ProjectGroup"][index] = tag_data["ProjectGroup"][tag_data.first_valid_index()]
                cal_data["CostDivision"][index] = tag_data["CostDivision"][tag_data.first_valid_index()]

        """


                # add InstanceType column
        #print "add InstanceType...."+ self.now()
        #cal_data["InstanceType"] = cal_data.apply(self.get_instance_type, axis=1)

        # add Platform column
        #print "add Platform......"+ self.now()
        #cal_data["Platform"] = cal_data.apply(self.get_platform, axis=1)


        print "add Platform and InstanceType......" + self.now()
        self.set_platform_instance_type(cal_data)


        # add null AdjustedCost column
        print "add AdjustedCost......"+ self.now()
        cal_data["AdjustedCost"] = cal_data.apply(self.default_adjust_cost, axis=1)

        # adjust rds ri cost
        print "adjust ri cost......"+ self.now()
        self.adjust_ri_cost(data = cal_data)


        print "calc TotalCost......"+ self.now()
        df_null = cal_data[(cal_data['user:Project'].isnull())]
        #df_totalcost = cal_data['AdjustedCost'].sum()
        #df_totalcost = pandas.to_numeric(df_totalcost)
        #df_nullcost = df_null['AdjustedCost'].sum()
        #df_nullcost = pandas.to_numeric(df_nullcost)
        sum_total = cal_data["AdjustedCost"].sum()
        sum_null = df_null["AdjustedCost"].sum()
        if sum_null == 0:
            null_rate = 1
        else:
            null_rate = sum_total / (sum_total - sum_null)

        self.get_bill_date(cal_data)

        cal_data.loc[:, 'NullRate'] = null_rate
        cal_data["TotalCost"] = cal_data.AdjustedCost * cal_data.NullRate
        null_data = cal_data[(cal_data["user:Project"]).isnull()]
        index = null_data.index
        cal_data["TotalCost"][index] = 0


        #cal_data["StartStamp"] = cal_data.apply(self.startstamp,axis=1)
        #cal_data["EndStamp"] = cal_data.apply(self.endstamp ,axis=1)

        print "set index of records ......" + self.now()
        self.set_index_of_records(cal_data)

        cols = self.config.calc_columns

        """
        cols = cal_data.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        cols.remove("RecordType")
        cols.remove("NullRate")
        cols.remove("user:Bill")
        cols.remove("user:billTag")
        cols.remove("user:Division")
        cols.remove("user:Name")
        cols.remove("user:Customer")
        #cols.remove("UsageStartDate")
        #cols.remove("UsageEndDate")
        """

        cal_data = cal_data[cols]

        cal_name = self.config.cal_prefix + month + ".csv"

        estimated_data = cal_data[cal_data["InvoiceID"]=="Estimated"]
        if len(estimated_data.index) > 0:
            cal_name = "estimated-" + month + ".csv"

        cal_file = os.path.join(self.config.cal_dir, cal_name)

        print "write to " + cal_name + " ......"+ self.now()
        cal_data.to_csv(cal_file, index=False,  sep=';')


        return cal_file, cal_name


    def cal_bills(self, month_list = []):
        """

        :param month_list:
        :return:
        """
        if len(month_list) > 0:
            for month in month_list:
                print "\n\n"
                print "calc [" + month + "] ......" + self.now()

                try:
                    """
                    try download bill and tag files, then cal and upload
                    """
                    # download bill file
                    bill_file = self.config.raw_prefix + month + ".csv"
                    bill_key = self.config.raw_folder + bill_file

                    obj = self.s3_resource.Object(self.config.proc_bucket, bill_key)
                    bill_file = os.path.join(self.config.raw_dir, bill_file)
                    if self.config.environment == "s3":
                        print "download " + bill_key + "......" +  self.now()
                        obj.download_file(bill_file)

                    # download tags file
                    tag_file = self.config.tag_prefix + month + ".csv"
                    tag_key = self.config.tag_folder + tag_file

                    obj = self.s3_resource.Object(self.config.proc_bucket, tag_key)
                    tag_file = os.path.join(self.config.tag_dir, tag_file)
                    if self.config.environment == "s3":
                        print "download " + tag_key
                        obj.download_file(tag_file)

                    # cal bill of this month
                    print "cal " + month + "......" + self.now()
                    cal_file, cal_name = self.cal_bill( month=month, \
                                                        bill_file=bill_file, \
                                                        tag_file = tag_file)

                    # copy bills for data analysis
                    if self.config.scope == "latest":
                        print "copy latest_month.csv......" +  self.now()
                        shutil.copy2(src= cal_file, \
                                     dst= os.path.join(self.config.cal_dir,"latest_month.csv"))

                    elif self.config.scope == "last":
                        print "copy last_month.csv......" + self.now()
                        shutil.copy2(src=cal_file, \
                                     dst=os.path.join(self.config.cal_dir, "last_month.csv"))

                    elif self.config.scope != "all":
                        print "copy this_month.csv......" + self.now()
                        shutil.copy2(src=cal_file, \
                                     dst=os.path.join(self.config.cal_dir, "this_month.csv"))


                    if self.config.environment == "s3":
                        print "upload " + cal_name + "......" + self.now()
                        # upload cal bill to s3 processed bucket
                        data = open(cal_file, 'rb')
                        s3key = self.config.cal_folder + cal_name
                        file_obj = self.s3_resource.Bucket( \
                            self.config.proc_bucket).put_object(Key=s3key, Body=data )

                except Exception as e:
                    """
                    process option error
                    """
                    #print ("open exception: %s: %s\n" % (e.args, e.message))
                    traceback.print_exc()

                print "finish [" + month + "] ......" + self.now()


def main():
    """
    main function for this script
    :return:
    """


    # get options
    cli = bill_cli.CommandLine()
    cli.get_options()

    # init Config instance
    config = cfg.Config(scope=cli.scope,config_yaml=cli.config_yaml, profile=cli.profile, \
                        environment= cli.environment, end_month = cli.end_month)


    # init AWS_Access instance
    aws = AWS_Access(config=config)
    print aws.now()
    print config.month_list

    aws.cal_bills(month_list=config.month_list)

    print aws.now()


if __name__ == '__main__':
    main()