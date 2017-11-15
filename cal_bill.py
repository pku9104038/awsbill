# -*- coding:utf-8 -*-
#
# Script for aws bill data processing and statistics
# Download raw bill and cost tag csv files
# Join them into cal-bills
# Upload to S3


import config as cfg
import bill_cli
import os
import datetime
import traceback
import pandas
import time
import hashlib

import csv,json


class AWS_Calc_Bill(object):
    """
    class for aws access and bill calculations
    """

    def __init__(self, config, commandline):
        """

        :param config:
        """
        self.config = config
        self.cli = commandline
        self.session = config.session
        self.s3_client = config.s3_client
        self.s3_resource = config.s3_resource


    def set_index(self,row):
        """

        :param row:
        :return:
        """
        """
        index =  str(row["RecordId"]) + "-" + str(row["SubscriptionId"]) + "-" +\
                str(row["RateId"]) +"-" + str(row["LinkedAccountId"]) + "-" + \
               str(row["InvoiceID"])
        """

        #index = str(row)
        index = str(row["RecordId"])
        hash =  hashlib.sha512(index).hexdigest()
        return hash[:8]

        #return hashlib.md5(str(row["RecordId"])).hexdigest()


    def set_index_of_records(self,data):
        """

        :param data:
        :return:
        """
        """
        null_recordid = data[data.RecordId.isnull()]
        index = null_recordid.index
        for i in index:
            data["RecordId"][i] = data["RateId"][i] # use rate id as record id

        nul_subscription = data[data.SubscriptionId.isnull()]
        index = nul_subscription.index
        for i in index:
            data["SubscriptionId"][i] = data["RateId"][i]  # use rate id as record id


        """
        self.cli.msg("Hash index......")
        data["index"] = data.apply(self.set_index, axis=1)


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
                    platform = "Redis"
                    #platform = "ElastiCache"
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
            elif product == "Amazon RDS Service" \
                    or product == "Amazon Relational Database Service":
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

                if desc.find("Linux") > -1 or desc.find("linux") > -1:
                    platform = "Linux"
                elif desc.find("RHEL") > -1:
                    platform = "RHEL"
                elif desc.find("Windows") > -1 or desc.find("windows") > -1:
                    platform = "Windows"
                elif desc.find("SQL Std") > -1:
                    platform = "SQL Std"
                elif desc.find("LoadBalancer") > -1 or desc.find("load balancer") > -1:
                    platform = "ELB"
                elif desc.find("Elastic IP") > -1:
                    platform = "EIP"
                #elif desc.find("CloudFront") > -1:
                #    platform = "CloudFront"
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
            data["InstanceType"][index] = usage_type #instance_type
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

        df = data[data["ItemDescription"] == "由于整合账单和小时行项目计算流程，该行项目包含舍入错误。"]
        index = df.index
        data["InstanceType"][index] = "Round"
        data["Platform"][index] = "Round"
        data["UsageType"][index] = "Round"

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

        grouped = data.groupby(["ProductName","Platform"]) #,"InstanceType"

        for name, group in grouped:
            product = name[0]

            # check product
            if product == "Amazon RDS Service" \
                or product == "Amazon ElastiCache" \
                or product == "Amazon Elastic Compute Cloud" \
                or product == "Amazon Relational Database Service" \
                or product == "Amazon Redshift":
                # check ri purchase order
                ripo = group[(group.ResourceId.isnull())]    #& (group.ReservedInstance == "Y")
                usage = group[~(group.ResourceId.isnull())] # | (group.ReservedInstance == "N")]

                # if ri purchaseed
                if len(ripo.index) > 0:
                    # total cost
                    cost = group["UnBlendedCost"].sum()
                    ri_cost = ripo["UnBlendedCost"].sum()
                    usage_cost = usage["UnBlendedCost"].sum()


                    # average cost
                    if ri_cost > 0 and usage_cost > 0:
                        rate = cost/usage_cost
                        null_rate = 0

                        # set AdjustedCost according to index
                        ripo_index = ripo.index
                        usage_index = usage.index
                        for idx in usage_index:
                            data["AdjustedCost"][idx] = data["UnBlendedCost"][idx] * rate

                        (data["AdjustedCost"])[ripo_index] = null_rate

            

    def get_bill_date(self,data):
        """

        :param data:
        :return:
        """

        this_month = data[data.Platform != "Support"]
        latest_start = this_month["UsageStartDate"].max()
        start_datetime = datetime.datetime.strptime(latest_start, \
                                                   "%Y-%m-%d %H:%M:%S")
        month = latest_start[:7]
        print month

        onehour = datetime.timedelta(hours=1)
        stop_datetime = start_datetime + onehour


        VAT = data[data.ItemDescription == "税金 VAT 类型"]

        vat_start_date = VAT["UsageStartDate"].min()
        start_datetime = datetime.datetime.strptime(vat_start_date, \
                                                  "%Y-%m-%d %H:%M:%S")

        vat_end_date = VAT["UsageEndDate"].max()
        end_datetime = datetime.datetime.strptime(vat_end_date, \
                                                  "%Y-%m-%d %H:%M:%S")
        onesecond = datetime.timedelta(seconds=1)
        end_datetime = end_datetime + onesecond


        left_datetime = end_datetime - stop_datetime
        hours = left_datetime.seconds / 60 / 60 + left_datetime.days * 24
        if hours > 0:
            self.cli.msg("Bill Time " + str(stop_datetime))
            self.cli.msg( "[ "+ str(hours) + " ] hours to the end of this month")

        return str(start_datetime), str(end_datetime), str(stop_datetime), month




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


    def read_month_trac_bill(self, month):
        """

        :param month:
        :return:
        """

        # download  file
        file = self.config.trac_prefix + month + ".csv"
        key = self.config.trac_folder + file

        obj = self.s3_resource.Object(self.config.proc_bucket, key)
        filepath = os.path.join(self.config.trac_dir, file)
        if self.config.environment == "s3":
            self.cli.msg("Download: " + key)
            obj.download_file(filepath)

        # read bill csv file into pandas dataframe
        self.cli.msg("Read: " + filepath)
        data = pandas.read_csv(filepath, \
                               dtype=self.config.calc_read_csv_dtype, \
                               low_memory=False)

        if self.config.remove == "yes":
            self.cli.msg("Remove: " + filepath)
            os.remove(filepath)

        return data

    def read_month_cost_tags(self, month):
        """

        :param month:
        :return:
        """

        # download  file
        file = self.config.tag_prefix + month + ".csv"
        key = self.config.tag_folder + file

        obj = self.s3_resource.Object(self.config.proc_bucket, key)
        filepath = os.path.join(self.config.tag_dir, file)
        if self.config.environment == "s3":
            self.cli.msg("Download: " + key)
            obj.download_file(filepath)

        # read bill csv file into pandas dataframe
        self.cli.msg("Read: " + filepath)
        data = pandas.read_csv(filepath, \
                               dtype=object, \
                               low_memory=False)

        if self.config.remove == "yes":
            self.cli.msg("Remove: " + filepath)
            os.remove(filepath)

        return data

    def check_estimated(self, data):
        """

        :param data:
        :return:
        """

        estimated = False
        grouped = data.groupby("InvoiceID")
        for name, group in grouped:
            if name == "Estimated":
                estimated = True
                break

        return estimated

    def save_month_calc_data(self, month, data):
        """

        :param month:
        :param data:
        :return:
        """

        name = self.config.cal_prefix + month + ".csv"

        if self.check_estimated(data=data):
            name = self.config.estimated_prefix + month + ".csv"

        file = os.path.join(self.config.cal_dir, name)
        self.cli.msg("Save as: " + file)
        data.to_csv(file, index=False, quoting=csv.QUOTE_ALL)

        s3key = self.config.cal_folder + name
        if self.config.environment == "s3":
            self.cli.msg("Upload: " + s3key)
            file_data = open(file, 'rb')
            file_obj = self.s3_resource.Bucket( \
                self.config.proc_bucket).put_object(Key=s3key, Body=file_data)

        if self.config.remove == "yes":
            self.cli.msg("Remove: " + file)
            os.remove(file)

        #self.get_bill_date(data)

    def cal_bill(self, data, tags_data):
        """

        :param data:
        :param tag_data:
        :return:
        """

        self.tag_metric_monitor_usage(data = data)

        # set dynamodb lost tag to ztjy only
        self.cli.msg("Tag DynamoDB......" )
        dynamo = data[data["ProductName"]=="Amazon DynamoDB"]
        idx = dynamo.index
        data["user:Project"][idx] = self.config.lost_tag["value"]


        # left join merge bill and tags
        self.cli.msg("Merge cost_tags......")
        calc_data = pandas.merge(left=data,right=tags_data, how="left", \
                                on=self.config.cost_tags_join_key)

        self.cli.msg("Add [Platform] [InstanceType]......")
        self.set_platform_instance_type(calc_data)


        # add null AdjustedCost column
        self.cli.msg("Add default [AdjustedCost]......")
        calc_data["AdjustedCost"] = calc_data.apply(self.default_adjust_cost, axis=1)

        # adjust rds ri cost
        self.cli.msg("Adjust ReservedInstance Cost......")
        self.adjust_ri_cost(data = calc_data)

        self.cli.msg("Calc TotalCost......")
        df_null = calc_data[(calc_data['user:Project'].isnull())]
        sum_total = calc_data["AdjustedCost"].sum()
        sum_null = df_null["AdjustedCost"].sum()
        if sum_null == 0:
            null_rate = 1
        else:
            null_rate = sum_total / (sum_total - sum_null)

        calc_data.loc[:, 'NullRate'] = null_rate
        calc_data["TotalCost"] = calc_data.AdjustedCost * calc_data.NullRate
        null_data = calc_data[(calc_data["user:Project"]).isnull()]
        index = null_data.index
        calc_data["TotalCost"][index] = 0



        # set bill start, end, stop time

        start_time, end_time, stop_time, bill_month = self.get_bill_date(data=calc_data)
        #print start_time, end_time, stop_time
        self.cli.msg("BillStop: "+stop_time)
        calc_data.loc[:, "BillStart"] = start_time
        calc_data.loc[:, "BillTerminate"] = end_time
        calc_data.loc[:, "BillStop"] = stop_time
        calc_data.loc[:, "BillCycle"] = bill_month

        # not need in redshift for a index
        #self.cli.msg("Set Primary Index......")
        #self.set_index_of_records(calc_data)

        cols = self.config.calc_columns
        calc_data = calc_data[cols]

        #print calc_data["BillStartTime"].max(),calc_data["BillStartTime"].min()
        #print calc_data["BillEndTime"].max(),calc_data["BillEndTime"].min()
        #print calc_data["BillStopTime"].max(),calc_data["BillStopTime"].min()

        return calc_data

    def cal_bills(self):
        """

        :param month_list:
        :return:
        """

        month_list = self.config.month_list
        self.cli.msg("CALC_BILL: ")
        #print self.config.month_list
        self.cli.msg(json.dumps(obj=self.config.month_list, indent=4))
        if len(month_list) > 0:
            for month in month_list:

                #print "\n"
                self.cli.msg("Start: " + month)

                try:
                    """
                    try download bill and tag files, then cal and upload
                    """

                    data = self.read_month_trac_bill(month=month)
                    tags_data = self.read_month_cost_tags(month=month)

                    # cal bill of this month
                    calc_data = self.cal_bill(data= data, tags_data=tags_data)

                    self.save_month_calc_data(month=month, data=calc_data)

                except Exception as e:
                    """
                    process option error
                    """
                    self.cli.msg("open exception: %s: %s\n" % (e.args, e.message))
                    traceback.print_exc()

                self.cli.msg("Finish: " + month)


        #print "\n"


def main():
    """
    main function for this script
    :return:
    """


    # get options
    cli = bill_cli.CommandLine()
    cli.get_options()

    # init Config instance
    config = cfg.Config(cli.option)


    # init AWS_Access instance
    aws_calc_bill = AWS_Calc_Bill(config=config, commandline=cli)

    aws_calc_bill.cal_bills()

    #print "\n"
    aws_calc_bill.cli.msg("You got it !  Cheers!")


if __name__ == '__main__':
    main()