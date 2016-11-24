# -*- coding:utf-8 -*-
#
# Script for aws bill data processing and statistics
# Trace back project tags for null resources

import pandas, csv
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

    def get_follow_up_month(self,month):
        """

        :param month:
        :return:
        """
        y = int(month[:4])
        m = int(month[5:])
        if m == 12 :
            m = 1
            y += 1
        else:
            m += 1

        return self.config.yyyy_mm(y,m)


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
        bill_data = pandas.read_csv(bill_file, dtype = object, low_memory=False)

        if self.config.remove == "yes":
            self.cli.msg("Remove: " + file)
            os.remove(bill_file)

        return bill_data

    def read_month_trac_data(self, month):
        """

        :param month:
        :return:
        """

        # download  file
        file = self.config.trac_prefix + month + ".csv"
        key = self.config.trac_folder + file

        obj = self.s3_resource.Object(self.config.proc_bucket, key)
        file = os.path.join(self.config.trac_dir, file)
        try:
            if self.config.environment == "s3":
                self.cli.msg("Download: " + key)
                obj.download_file(file)

            # read bill csv file into pandas dataframe
            self.cli.msg("Read: " + file)

            data = pandas.read_csv(file, dtype=object, low_memory=False)
            if self.config.remove == "yes":
                self.cli.msg("Remove: " + file)
                os.remove(file)

            return False, data
        except Exception as e:
            """
            process option error
            """
            print ("open exception: %s: %s\n" %(e.args, e.message))
            return True, None


    def save_month_trac_data(self,month, data):
        """

        :param month:
        :param data:
        :return:
        """

        name = self.config.trac_prefix + month + ".csv"
        file = os.path.join(self.config.trac_dir, name)
        self.cli.msg("Save as: " + file)
        data.to_csv(file, index=False, quoting= csv.QUOTE_ALL)

        s3key = self.config.raw_folder + name
        if self.config.environment == "s3":
            self.cli.msg("Upload: " + s3key)
            data = open(file, 'rb')
            file_obj = self.s3_resource.Bucket( \
                self.config.proc_bucket).put_object(Key=s3key, Body=data)

        if self.config.remove == "yes":
            self.cli.msg("Remove: " + file)
            os.remove(file)

    def tag_by_resourceid(self, data = None, follow_up_data = None, first_month = True):
        """

        :param data:
        :return:
        """


        # check in this month data
        all_data = data
        grouped = data.groupby("ResourceId")
        for name, group in grouped:

            null_data = group[(group["user:Project"].isnull())]
            index = null_data.index
            if len(index) > 0:
                resource_data = all_data[all_data["ResourceId"] == name]
                project_data = resource_data[~(resource_data["user:Project"].isnull())]
                if len(project_data.index) > 0:
                    project = project_data["user:Project"][project_data["user:Project"].first_valid_index()]
                    data["user:Project"][index] = project

        if  not first_month: # check it again use follow_up month data
            all_data = follow_up_data
            grouped = data.groupby("ResourceId")
            for name, group in grouped:
                null_data = group[(group["user:Project"].isnull())]
                index = null_data.index
                if len(index) > 0:
                    resource_data = all_data[all_data["ResourceId"] == name]
                    project_data = resource_data[~(resource_data["user:Project"].isnull())]
                    if len(project_data.index) > 0:
                        project = project_data["user:Project"][project_data["user:Project"].first_valid_index()]
                        data["user:Project"][index] = project

    def trace_bills(self):
        """

        :param month_list:
        :return:
        """

        month_list = self.config.month_list
        print month_list

        length = len(month_list)
        if length > 0:
            first_month = True
            follow_up_data = None
            for l in range(1,length+1):
                month = month_list[length-l]
                print "\n"
                self.cli.msg("Start: " + month)

                data = self.read_month_raw_data(month)
                follow_up_month  = self.get_follow_up_month(month)

                first_month,follow_up_data = self.read_month_trac_data(follow_up_month)

                self.cli.msg("Tracing: " + month)
                self.tag_by_resourceid(data=data, \
                                           first_month=first_month, \
                                           follow_up_data=follow_up_data)
                #follow_up_data = data
                #first_month = False
                self.save_month_trac_data(month=month, data=data)

                self.cli.msg("Finish: " + month)




def main():
    """
    main function for this script
    :return:
    """

    # get options
    cli = bill_cli.CommandLine()
    cli.get_options()

    # init config
    config = cfg.Config(cli.option)

    # init AWS_Access instance
    aws_trace_bill = AWS_Trace_Bill(config=config, commandline = cli)

    aws_trace_bill.trace_bills()

    print "\n"
    aws_trace_bill.cli.msg("You got it !  Cheers! \n")


if __name__ == '__main__':
    main()