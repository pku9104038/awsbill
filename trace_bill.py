# -*- coding:utf-8 -*-
#
# Script for aws bill data processing and statistics
# Trace back project tags for null resources

import pandas
import bill_cli
import config as cfg
import time, sys, os

class AWS_Trace_Bill(object):
    """

    """

    def __init__(self, config, commandline):
        """
        init attributes
        """
        self.config = config
        self.session = config.session
        self.s3_client = config.s3_client
        self.s3_resource = config.s3_resource
        self.cli = commandline


    def read_month_raw_data(self,month):
        """

        :param month:
        :return:
        """

        # download bill file
        bill_file = self.config.raw_prefix + month + ".csv"
        bill_key = self.config.raw_folder + bill_file

        obj = self.s3_resource.Object(self.config.proc_bucket, bill_key)
        bill_file = os.path.join(self.config.raw_dir, bill_file)
        if self.config.environment == "s3":
            self.cli.msg("Download: " + bill_key)
            obj.download_file(bill_file)

        # read bill csv file into pandas dataframe
        self.cli.msg("Read: " + bill_file)
        bill_data = pandas.read_csv(bill_file, dtype={"InvoiceID": object}, low_memory=False)

        return bill_data

    def save_month_raw_data(self,month, data):
        """

        :param month:
        :param data:
        :return:
        """

        raw_name = self.config.raw_prefix + month + ".csv"
        raw_file = os.path.join(self.config.raw_dir, raw_name)
        self.cli.msg("Save as: " + raw_file)
        data.to_csv(raw_file, index=False, sep=';')

        s3key = self.config.raw_folder + raw_name
        if self.config.environment == "s3":
            self.cli.msg("Upload: " + s3key)
            data = open(raw_file, 'rb')
            file_obj = self.s3_resource.Bucket( \
                self.config.proc_bucket).put_object(Key=s3key, Body=data)


    def tag_by_resourceid(self, data = None, follow_up_data = None, first_month = True):
        """

        :param data:
        :return:
        """
        if first_month:
            all_data = data
        else:
            all_data = pandas.concat([data,follow_up_data])

        grouped = all_data.groupby("ResourceId")

        for name, group in grouped:
            resourceid = name[0]
            bull_data = group[(group["user:Project"].isnull())]
            index = bull_data.index
            if len(index) > 0:
                project_data = group[~(group["user:Project"].isnull())]
                if len(project_data.index) > 0:
                    project = project_data["user:Project"][project_data.first_valid_index()]
                    data["user:Project"][index] = project

    def trace_bills(self, month_list = []):
        """

        :param month_list:
        :return:
        """
        print month_list
        length = len(month_list)
        if length > 0:
            first_month = True
            follow_up_data = None
            for l in range(1,length+1):
                month = month_list[length-l]
                data = self.read_month_raw_data(month)

                self.tag_by_resourceid(data=data, \
                                           first_month=first_month, \
                                           follow_up_data=follow_up_data)
                follow_up_data = data
                first_month = False
                self.save_month_raw_data(month=month, data=data)




def main():
    """
    main function for this script
    :return:
    """

    # get options
    cli = bill_cli.CommandLine()
    cli.get_options()

    # init config
    config = cfg.Config(scope=cli.scope,config_yaml=cli.config_yaml, \
                        profile=cli.profile, environment= cli.environment)

    if config.scope != "all" and config.scope != "latest" \
            and config.scope != "last":
        now = time.localtime(time.time())
        yyyy_mm = str(now.tm_year) + "-" + str(now.tm_mon)
        start_month = config.scope
        config.scope = "all"
        config.month_list = config.get_months(start_yyyy_mm=start_month, \
                                              end_yyyy_mm=yyyy_mm)

    # init AWS_Access instance
    aws_trace_bill = AWS_Trace_Bill(config=config, commandline = cli)

    aws_trace_bill.trace_bills(month_list=aws_trace_bill.config.month_list)

    print "\n"
    aws_trace_bill.cli.msg("You got it !  Cheers! \n")


if __name__ == '__main__':
    main()