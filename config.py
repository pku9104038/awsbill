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


    def list_months(self):
        """

        :return:
        """

        sta_year = int(self.start_month[:4])
        sta_mon = int(self.start_month[5:])

        end_year = int(self.end_month[:4])
        end_mon = int(self.end_month[5:])
        month_list = []
        if (end_mon <= 12 and sta_year >= 2006 and sta_mon <= 12) and \
                ((end_year > sta_year) or \
                         (end_year == sta_year and end_mon >= sta_mon)):

            year = sta_year
            month = sta_mon

            while True:
                month_list.append(self.yyyy_mm(year=year, month=month))
                if year == end_year and month == end_mon:
                    break
                else:
                    if month == 12:
                        month = 1
                        year = year + 1
                    else:
                        month = month + 1

        return month_list

    def get_months(self):
        """
        get month list according to the scope option
        :return:
        """

        month_list = []

        start_month = self.cost_start
        end_month = self.end_month

        now = time.localtime(time.time())
        year = now.tm_year
        month = now.tm_mon


        if self.scope == "last":
            if month == 1:
                month = 12
                year = year - 1
            else:
                month = month - 1
            month_list.append(self.yyyy_mm(year=year,month=month))
        elif self.scope == "latest":
            month_list.append(self.yyyy_mm(year=year,month=month))
        else:
            if self.scope == "all":
                end_month = self.yyyy_mm(year,month)
            else:
                start_month = self.scope
            print (start_month, end_month)


            sta_year = int(start_month[:4])
            sta_mon = int(start_month[5:])

            end_year = int(end_month[:4])
            end_mon = int(end_month[5:])

            if ( end_mon <= 12 and sta_year >= 2006 and sta_mon <= 12) and \
                    ( (end_year > sta_year) or \
                        (end_year == sta_year and end_mon >= sta_mon) ):

                year = sta_year
                month = sta_mon

                while True:
                        month_list.append(self.yyyy_mm(year=year, month=month))
                        if year == end_year and month == end_mon:
                            break
                        else:
                            if month == 12:
                                month = 1
                                year = year + 1
                            else:
                                month = month + 1

        return  month_list


    def __init__(self, opt):
        """

        :param scope:
        :param profile:
        :param config_yaml:
        :param operate:
        """


        self.scope = opt.scope
        self.config_file = opt.config_yaml
        self.environment = opt.environment
        self.end_month = opt.end_month
        self.profile = opt.profile
        self.remove = opt.remove


        self.yaml_obj = yaml.load(open(self.config_file))

        # get bill_logs
        bill_logs = self.yaml_obj.get("bill_logs")
        self.log_bucket = bill_logs["s3_bucket"]["bucket_name"]
        self.log_prefix = bill_logs["s3_bucket"]["log_prefix"]

        # get bill_processed
        bill_processed = self.yaml_obj.get("bill_processed")
        self.proc_bucket = bill_processed["s3_bucket"]["bucket_name"]

        # s3 folder and local dir
        directories = self.yaml_obj.get("directories")
        self.cwd = os.path.abspath(os.path.curdir)

        self.tmp_dir = os.path.join(self.cwd, directories["tmp"])
        if(not os.path.exists(self.tmp_dir)):
            os.mkdir(self.tmp_dir)

        #self.data_dir = os.path.join(self.cwd, directories["data"])
        #if (not os.path.exists(self.data_dir)):
        #    os.mkdir(self.data_dir)

        self.raw_folder = bill_processed["s3_bucket"]["raw_folder"]
        self.raw_prefix = bill_processed["s3_bucket"]["raw_prefix"]
        self.raw_dir = os.path.join(self.cwd, directories["raw"])
        if (not os.path.exists(self.raw_dir)):
            os.mkdir(self.raw_dir)

        self.tag_folder = bill_processed["s3_bucket"]["tag_folder"]
        self.tag_prefix = bill_processed["s3_bucket"]["tag_prefix"]
        self.tag_dir = os.path.join(self.cwd, directories["tag"])
        if (not os.path.exists(self.tag_dir)):
            os.mkdir(self.tag_dir)

        self.cal_folder = bill_processed["s3_bucket"]["cal_folder"]
        self.cal_prefix = bill_processed["s3_bucket"]["cal_prefix"]
        self.estimated_prefix = bill_processed["s3_bucket"]["estimated_prefix"]
        self.cal_dir = os.path.join(self.cwd, directories["cal"])
        if (not os.path.exists(self.cal_dir)):
            os.mkdir(self.cal_dir)

        self.stat_folder = bill_processed["s3_bucket"]["stat_folder"]
        self.stat_prefix = bill_processed["s3_bucket"]["stat_prefix"]
        self.instances_prefix = bill_processed["s3_bucket"]["instances_prefix"]
        self.stat_dir = os.path.join(self.cwd, directories["stat"])
        if (not os.path.exists(self.stat_dir)):
            os.mkdir(self.stat_dir)

        self.trac_folder = bill_processed["s3_bucket"]["trac_folder"]
        self.trac_prefix = bill_processed["s3_bucket"]["trac_prefix"]
        self.trac_dir = os.path.join(self.cwd, directories["trac"])
        if (not os.path.exists(self.trac_dir)):
            os.mkdir(self.trac_dir)

        # tags
        self.bill_tags = self.yaml_obj.get("bill_columns")["user_tags"]
        self.lost_tag = self.yaml_obj.get("lost_tag")

        # cost tags
        self.cost_tags_file = self.yaml_obj.get("cost_tags")["tags_file"]
        self.start_month = self.cost_start = self.yaml_obj.get("cost_tags")["start_month"]
        self.cost_tags_join_key = self.yaml_obj.get("cost_tags")["join_key"]

        # resource tags
        self.resource_tags_file = self.yaml_obj.get("resource_tags")["tags_file"]
        self.resource_tags_id_key = self.yaml_obj.get("resource_tags")["id_key"]
        self.resource_tags_prj_key = self.yaml_obj.get("resource_tags")["prj_key"]

        # bill columns
        self.raw_columns = self.yaml_obj.get("bill_columns")["raw"]
        self.calc_columns = self.yaml_obj.get("bill_columns")["calc"]
        self.calc_read_csv_dtype =self.yaml_obj.get("bill_columns")["calc_read_csv_dtype"]


        # init aws_session
        self.session = boto3.Session(profile_name=self.profile)

        # Any clients created from this session will use credentials
        # from the [profile] section of ~/.aws/credentials.

        # init s3_client, s3_resource
        self.s3_client = self.session.client('s3')
        self.s3_resource = self.session.resource('s3')

        # scope month_list
        now = time.localtime(time.time())
        yyyy_mm = str(now.tm_year)+"-"+str(now.tm_mon)
        self.month_list = self.get_months()

        # merged bill
        self.merge_file = self.yaml_obj.get("statistics")["merge_file"]

        # Redshift config
        self.redshift_host = self.yaml_obj.get("redshift")["host"]
        self.redshift_port = self.yaml_obj.get("redshift")["port"]
        self.redshift_user = self.yaml_obj.get("redshift")["user"]
        self.redshift_pwd = self.yaml_obj.get("redshift")["passwd"]
        self.redshift_db = self.yaml_obj.get("redshift")["database"]
        self.redshift_t_history = self.yaml_obj.get("redshift")["tables"]["history"]
        self.redshift_t_year = self.yaml_obj.get("redshift")["tables"]["year"]
        self.redshift_t_month = self.yaml_obj.get("redshift")["tables"]["month"]
        self.redshift_t_estimated = self.yaml_obj.get("redshift")["tables"]["estimated"]
        self.redshift_t_tmp = self.yaml_obj.get("redshift")["tables"]["tmp"]
        self.s3_credentials = self.yaml_obj.get("redshift")["credentials"]












