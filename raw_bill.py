# -*- coding:utf-8 -*-
#
# Script for aws bill data processing and statistics
# Download aws-billing-detailed-line-items-with-resources-and-tags zip file
# Unzip and add tags project tag for the first two bills from 2014.12


import zipfile as zip
import pandas
import os
import csv

import bill_cli
import config as cfg
import json

class AWS_Raw_Bill(object):
    """
    calss for AWS_Access session, clients, resources
    """
    def __init__(self, config, commandline):
        """
        init attributes
        """

        self.config = config
        self.cli = commandline

        self.session = config.session
        self.s3_client = config.s3_client
        self.s3_resource = config.s3_resource

    def trim_project_tag(self,row):
        """

        :param row:
        :return:
        """

        project = row["user:Project"]
        if not project == None:
            project = str(project).strip()
        return project

    def proc_detail_tags_bills(self):
        """

        :return:
        """

        self.cli.msg("RAW_BILL: ")
        #print self.config.month_list
        self.cli.msg(json.dumps(obj=self.config.month_list, indent=4))

        for month in self.config.month_list:
            #print "\n"
            self.cli.msg("Start: " + month)
            zip_name = self.config.log_prefix + month + ".csv.zip"
            zip_file = os.path.join(self.config.tmp_dir,zip_name)
            if self.config.environment == "s3":
                self.cli.msg("Download: " + zip_name)
                obj = self.s3_resource.Object(self.config.log_bucket, zip_name)
                obj.download_file(zip_file)


            # process zip file
            zFile = zip.ZipFile(zip_file, 'r')
            for filename in zFile.namelist():
                # unzip log csv file
                self.cli.msg("Unzip: " + filename[len(self.config.log_prefix):])
                data = zFile.read(filename)
                filepath = os.path.join(self.config.tmp_dir, \
                                         filename[len(self.config.log_prefix):])
                file = open(filepath, 'w+b')
                file.write(data)
                file.close()

                # read unziped log csv file into pandas dataframe
                data = pandas.read_csv(file.name, dtype = object, low_memory=False)
                # remove tmp csv
                if self.config.remove == "yes":
                    self.cli.msg("Remove: " + filepath)
                    os.remove(filepath)

                # remove records not 'LineItem'
                data = data[(data['RecordType'] == 'LineItem')]

                # check columns for lost user tags and add them
                # tag first two month with ztjy
                columns = list(data.columns.values)
                for tag in self.config.bill_tags:
                    if not (tag in columns):
                        if tag == self.config.lost_tag["key"]:
                            data.loc[:, tag] = self.config.lost_tag["value"]  # this not work
                        else:
                            data.loc[:, tag] = None

                self.cli.msg("Trim : [user:Project]")
                data["user:Project"] = data.apply(self.trim_project_tag, axis=1)

                # save to local csv
                csvname = self.config.raw_prefix + filename[len(self.config.log_prefix):]
                csvpath = os.path.join(self.config.raw_dir, csvname)
                self.cli.msg("Save as: " + csvpath)
                columns = self.config.raw_columns
                data = data[columns]
                data.to_csv(csvpath, index=False, quoting= csv.QUOTE_ALL)
                # use sep=";" will cause pandas.io.common.CParserError: Error tokenizing data
                # change to quoting= csv.QUOTE_ALL

                # upload to s3 processed bucket
                if self.config.environment == "s3":
                    data = open(csvpath, 'rb')
                    s3key = self.config.raw_folder + csvname
                    self.cli.msg("Upload: " + s3key)
                    file_obj = self.s3_resource.Bucket(self.config.proc_bucket).\
                        put_object(Key=s3key, Body=data)

                # remove local csv
                if self.config.remove == "yes":
                    self.cli.msg("Remove: " + csvpath)
                    os.remove(csvpath)

            self.cli.msg("Finish: " + month)

            # remove local zip_file
            if self.config.remove == "yes":
                self.cli.msg("Remove: " + zip_file)
                os.remove(zip_file)

        #print "\n"


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
    aws_raw_bill = AWS_Raw_Bill(config=config, commandline=cli)
    aws_raw_bill.proc_detail_tags_bills()


    #print "\n"
    aws_raw_bill.cli.msg("You got it !  Cheers!")

if __name__ == '__main__':
    main()