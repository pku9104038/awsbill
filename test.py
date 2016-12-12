# -*- coding:utf-8 -*-

import bill_cli
import config as cfg
import time

import json

import cal_bill as calc
import tag_bill as tag

def main():
    """
    main function for this script
    :return:
    """

    # get options
    cli = bill_cli.CommandLine()

    cli.get_options()

    cli.msg("I am here")

    config = cfg.Config(cli.option)


    aws_tag = tag.AWS_Bill_Tag(config=config,commandline=cli)
    aws_tag.merge_tags_datas(month="2016-09")






if __name__ == '__main__':
    main()