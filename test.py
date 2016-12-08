# -*- coding:utf-8 -*-

import bill_cli
import config as cfg
import time

import json

import cal_bill as calc

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

    calc_bill = calc.AWS_Calc_Bill(config=config, commandline=cli)







if __name__ == '__main__':
    main()