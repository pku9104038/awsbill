# -*- encoding:utf-8 -*-

import bill_cli as cli
import config as cfg
import raw_bill as raw
import trac_bill as trac
import tag_bill as tag
import cal_bill as calc
import load_bill as load
import time

class AWS_Auto_Bill(object):
    """

    """
    def __init__(self, commandline, config):
        """

        :param commandline:
        :param config:
        """
        self.cli = commandline
        self.config = config

        self.session = config.session
        self.s3_client = config.s3_client
        self.s3_resource = config.s3_resource

        self.raw_bill = raw.AWS_Raw_Bill(config=config,commandline=commandline)
        self.trac_bill = trac.AWS_Trace_Bill(commandline=commandline, \
                                            config=config)
        self.tag_bill = tag.AWS_Bill_Tag(commandline=commandline,\
                                         config=config)
        self.calc_bill = calc.AWS_Calc_Bill(commandline=commandline,\
                                            config=config)
        self.load_bill = load.AWS_Load_Bill(commandline=commandline,\
                                            config=config)

        self.trace_back_month = 3


    def check_raw_zip(self, month):
        """

        :return:
        """

        bucket = self.s3_resource.Bucket(self.config.log_bucket)
        key = self.config.log_prefix + month + ".csv.zip"
        objs = list(bucket.objects.filter(Prefix=key))

        if len(objs) > 0 and objs[0].key == key:
            return True
        else:
            return False

    def check_calc_csv(self,month):
        """

        :param month:
        :return:
        """

        bucket = self.s3_resource.Bucket(self.config.proc_bucket)
        key = self.config.cal_folder+self.config.cal_prefix + month + ".csv.zip"
        objs = list(bucket.objects.filter(Prefix=key))

        if len(objs) > 0 and objs[0].key == key:
            return True
        else:
            return False

    def get_previous_month(self, month):
        """

        :param month:
        :return:
        """
        year = int(month[:4])
        mon = int(month[5:])

        if mon == 1:
            mon = 12
            year = year-1
        else:
            mon = mon-1

        return self.config.yyyy_mm(year=year,month=mon)


    def check_latest_month(self):
        """

        :return:
        """

        now = time.localtime(time.time())
        year = now.tm_year
        month = now.tm_mon

        self.config.month_list = []

        curr_month = self.config.yyyy_mm(year=year,month=month)
        if self.check_raw_zip(curr_month):
            self.config.end_month = curr_month
        else:
            self.config.end_month = self.get_previous_month(curr_month)

        if not (self.check_calc_csv(self.config.end_month)):
            self.config.start_month= self.get_previous_month(self.config.end_month)
            self.config.month_list.append(self.config.start_month)
        else:
            self.config.start_month = self.config.end_month

        self.config.month_list.append(self.config.end_month)

    def check_trace_back_month(self,n):
        """

        :param n:
        :return:
        """
        i = n
        self.config.month_list = []
        self.config.start_month = self.config.end_month
        while True:
            if i < 1:
                break
            else:
                i = i-1
                self.config.start_month = self.get_previous_month(self.config.start_month)

        self.config.month_list = self.config.list_months()


    def raw_bills(self):
        """

        :return:
        """
        self.raw_bill.proc_detail_tags_bills()

    def trac_bills(self):
        """

        :return:
        """

        self.trac_bill.trace_bills()

    def tag_bills(self):
        """

        :return:
        """
        self.tag_bill.tags_bills()


    def calc_bills(self):
        """

        :return:
        """

        self.calc_bill.cal_bills()

    def load_bills(self):
        """

        :return:
        """
        self.load_bill.load_latest_bills()



def main():

    commandline = cli.CommandLine()
    config = cfg.Config(commandline.option)

    aws_auto_bill = AWS_Auto_Bill(commandline=commandline, config=config)

    aws_auto_bill.check_latest_month()
    aws_auto_bill.raw_bills()

    #aws_auto_bill.check_trace_back_month(n=aws_auto_bill.trace_back_month)
    aws_auto_bill.trac_bills()
    aws_auto_bill.tag_bills()
    aws_auto_bill.calc_bills()
    aws_auto_bill.load_bills()

if __name__ == "__main__":
    main()