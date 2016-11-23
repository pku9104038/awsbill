# -*- coding:utf-8 -*-
#
# Script for aws bill data processing and statistics
# Download aws-billing-detailed-line-items-with-resources-and-tags zip file
# Unzip and add tags project tag for the first two bills from 2014.12


import zipfile as zip
import pandas
import os
import numpy

import bill_cli
import config as cfg

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

    def proc_detail_tags_bills(self):
        """

        :return:
        """

        for month in self.config.month_list:
            print "\n"
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
                file = open(os.path.join(self.config.tmp_dir, \
                                         filename[len(self.config.log_prefix):]), 'w+b')
                file.write(data)
                file.close()

                # read unziped log csv file into pandas dataframe
                data = pandas.read_csv(file.name, dtype=object, low_memory=False)

                # remove records not 'LineItem'
                df = data[(data['RecordType'] == 'LineItem')]

                # check columns for lost tags and add them for the firs two months logs
                columns = list(df.columns.values)
                for tag in self.config.bill_tags:
                    if not (tag["tag"] in columns):
                        if tag["tag"] == self.config.lost_tag["key"]:
                            df.loc[:,tag["tag"]] = self.config.lost_tag["value"] # this not work
                        else:
                            df.loc[:,tag["tag"]] = numpy.nan

                # save to local csv
                csvname = self.config.raw_prefix + filename[len(self.config.log_prefix):]
                csvpath = os.path.join(self.config.raw_dir, csvname)

                cols = self.config.raw_columns
                df = df[cols]
                self.cli.msg("Save as: " + csvpath)
                df.to_csv(csvpath, index=False, sep=";")

                # upload to s3 processed bucket
                if self.config.environment == "s3":
                    data = open(csvpath, 'rb')
                    s3key = self.config.raw_folder + csvname
                    self.cli.msg("Upload: " + s3key)
                    file_obj = self.s3_resource.Bucket(self.config.proc_bucket).\
                        put_object(Key=s3key, Body=data)

                # remove local csv
                # os.remove(csvpath)

            self.cli.msg("Finish: " + month)

            # remove zipfile
            #os.remove(zip_file)


def main():
    """
    main function for this script
    :return:
    """
    # get options
    cli = bill_cli.CommandLine()
    cli.get_options()

    # init config
    config = cfg.Config(scope=cli.scope, config_yaml=cli.config_yaml, \
                        profile=cli.profile, environment=cli.environment)


    # init AWS_Access instance
    aws_raw_bill = AWS_Raw_Bill(config=config, commandline=cli)
    aws_raw_bill.proc_detail_tags_bills()


    print "\n"
    aws_raw_bill.cli.msg("You got it !  Cheers! \n")

if __name__ == '__main__':
    main()