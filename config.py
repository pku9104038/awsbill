# -*- coding:utf-8 -*-
#
# Script for aws bill data processing and statistics
# define Config class

import yaml
import os
import boto3
import time

class Config(object):
    """
    calss for process environments configurations
    """

    # format (year,month) into yyyy-mm
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
        get month list according to the scope option
        :return:
        """

        month_list = []
        start_month = self.cost_start
        now = time.localtime(time.time())
        cur_year = now.tm_year
        cur_mon = now.tm_mon
        sta_year = int(start_month[:4])
        sta_mon = int(start_month[5:])
        year = cur_year
        month = cur_year

        if self.scope == "all":
            year = sta_year
            month = sta_mon
            end_year = cur_year
            end_mon = cur_mon
            while True:

                month_list.append(self.yyyy_mm(year=year,month=month))

                if year == end_year and month == end_mon:
                    break
                else:
                    if month == 12:
                        month = 1
                        year = year + 1
                    else:
                        month = month + 1
        elif self.scope == "origin":
            year = year
        elif self.scope == "last":
            if cur_mon == 1:
                month = 12
                year = cur_year - 1
            else:
                month = cur_mon - 1
                year = cur_year
            month_list.append(self.yyyy_mm(year=year,month=month))
        elif self.scope == "latest":
            year = cur_year
            month = cur_mon
            month_list.append(self.yyyy_mm(year=year,month=month))
        else:
            month_list.append(self.scope)

        return  month_list


    def __init__(self, scope, profile, config_yaml, operate=None):
        """

        :param scope:
        :param profile:
        :param config_yaml:
        :param operate:
        """
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
        self.cost_tags_join_key = self.yaml_obj.get("cost_tags")["join_key"]

        # aws seesion
        self.profile = profile

        # init aws_session
        self.session = boto3.Session(profile_name=profile)

        # Any clients created from this session will use credentials
        # from the [profile] section of ~/.aws/credentials.

        # init s3_client, s3_resource
        self.s3_client = self.session.client('s3')
        self.s3_resource = self.session.resource('s3')

        # scope month_list
        self.month_list = self.get_months()

        # merged bill
        self.merge_file = self.yaml_obj.get("statistics")["merge_file"]




