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
import json


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

    def read_cost_tags(self):
        """

        :return:
        """

        # download  file
        file = self.config.cost_tags_file
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


        return data

    def read_cost_tags_month(self,month):
        """

        :return:
        """

        # download  file
        file = self.config.tag_prefix+month+".csv"
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

        return data

    def merge_tags_datas(self,month):
        """

        :param month:
        :return:
        """
        month_tags = self.read_cost_tags_month(month=month)
        print month
        print month_tags
        month_tags["index"] = month_tags["user:Project"]
        month_data = month_tags.set_index(keys="index")

        cost_tags = self.read_cost_tags()
        cost_tags["index"] = cost_tags["user:Project"]

        cost_data = cost_tags.set_index(keys="index")
        print "\n cost"

        print cost_tags

        #merged_tags = pandas.merge(left=month_tags, right=cost_tags, how="outer", \
        #                      on=self.config.cost_tags_join_key)
        print "\n merge"

        #merged_tags = pandas.concat([month_data, cost_data], axis=1)
        merged_tags = month_data.append(cost_data)
        print merged_tags


    def tags_bills(self):
        """

        :param month_list:
        :return:
        """


        self.cli.msg("TAG_BILL: ")
        #print self.config.month_list
        self.cli.msg(json.dumps(obj=self.config.month_list, indent=4))
        month_list = self.config.month_list
        if len(month_list) > 0:
            # read cost tags file into pandas dataframe
            #tag_file = os.path.join(self.config.tag_dir,
            #                       self.config.cost_tags_file)
            #self.cli.msg("Read: " + tag_file)
            #tags_pools = pandas.read_csv(tag_file, dtype = object, low_memory=False)
            tags_pools = self.read_cost_tags()

            for month in month_list:
                #print "\n"
                self.cli.msg("Start: " + month)
                try:
                    """
                    try download bill and tag files, then cal and upload
                    """
                    # download bill file if scope = s3
                    bill_file = self.config.trac_prefix + month + ".csv"
                    bill_key = self.config.trac_folder + bill_file
                    obj = self.s3_resource.Object(self.config.proc_bucket, bill_key)
                    bill_file = os.path.join(self.config.trac_dir, bill_file)
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
                        self.cli.msg("Merge cost tags: " + bill_key)
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

    # init config
    config = cfg.Config(cli.option)

    # init AWS_Access instance
    aws_bill_tag = AWS_Bill_Tag(config=config, commandline = cli)

    aws_bill_tag.tags_bills()

    #print "\n"
    aws_bill_tag.cli.msg("You got it !  Cheers!")


if __name__ == '__main__':
    main()