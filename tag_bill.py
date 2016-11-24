# -*- coding:utf-8 -*-
#
# Script for aws bill data processing and statistics
# Download raw bill csv from s3
# Extract tags from raw bill csv
# Left join merge the bill user:Project and cost_tags CostDivision, ProjectGroup
# Upload tags file to s3


import pandas, csv
import os
import traceback
import bill_cli, config as cfg


class AWS_Bill_Tag(object):
    """
    calss for AWS_Access session, clients, resources
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


    def tags_bills(self, month_list=[]):
        """

        :param month_list:
        :return:
        """
        if len(month_list) > 0:
            # read cost tags file into pandas dataframe
            tag_file = os.path.join(self.config.tag_dir,
                                    self.config.cost_tags_file)
            self.cli.msg("Read: " + tag_file)
            tags_pools = pandas.read_csv(tag_file, dtype = object, low_memory=False)
            for month in month_list:
                print "\n"
                self.cli.msg("Start: " + month)
                try:
                    """
                    try download bill and tag files, then cal and upload
                    """
                    # download bill file if scope = s3
                    bill_file = self.config.raw_prefix + month + ".csv"
                    bill_key = self.config.raw_folder + bill_file
                    obj = self.s3_resource.Object(self.config.proc_bucket, bill_key)
                    bill_file = os.path.join(self.config.raw_dir, bill_file)
                    if self.config.environment == "s3":
                        self.cli.msg("Download: " + bill_key)
                        obj.download_file(bill_file)

                    # read bill csv file into pandas dataframe
                    self.cli.msg("Read: " + bill_file)
                    bill_data = pandas.read_csv(bill_file, \
                                                dtype=object, \
                                                low_memory=False)

                    tags_data = pandas.DataFrame(columns=["user:Project"])
                    index = 1
                    grouped = bill_data.groupby(["user:Project"])
                    for name, group in grouped:
                        tags_data.loc[index,"user:Project"] = name
                        index += 1

                    if len(tags_data.index) > 0:
                        month_data = pandas.merge(left=tags_data, right=tags_pools, how="left", \
                                                on=self.config.cost_tags_join_key)
                        tag_name = self.config.tag_prefix + month + ".csv"
                        tag_file = os.path.join(self.config.tag_dir, tag_name)
                        self.cli.msg("Save as: " + tag_file)
                        month_data.to_csv(tag_file, index=False, quoting= csv.QUOTE_ALL)

                        if self.config.environment == "s3":
                            tag_key = self.config.tag_folder + tag_name
                            self.cli.msg("Upload: " + tag_key)
                            data = open(tag_file, 'rb')
                            file_obj = self.s3_resource.Bucket( \
                                self.config.proc_bucket).put_object(Key=tag_key, Body=data)



                except Exception as e:
                    """
                    process option error
                    """
                    # print ("open exception: %s: %s\n" % (e.args, e.message))
                    traceback.print_exc()

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
    config = cfg.Config(scope=cli.scope,config_yaml=cli.config_yaml, \
                        profile=cli.profile, environment= cli.environment, \
                        end_month=cli.end_month)

    # init AWS_Access instance
    aws_bill_tag = AWS_Bill_Tag(config=config, commandline = cli)

    aws_bill_tag.tags_bills(aws_bill_tag.config.month_list)

    print "\n"
    aws_bill_tag.cli.msg("You got it !  Cheers! \n")


if __name__ == '__main__':
    main()