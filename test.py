# -*- coding:utf-8 -*-

import bill_cli
import config as cfg
import time

def main():
    """
    main function for this script
    :return:
    """

    # get options
    cli = bill_cli.CommandLine()

    cli.get_options()

    cli.msg("I am here")

    config = cfg.Config(scope=cli.scope, end_month=cli.end_month)


    print config.month_list


if __name__ == '__main__':
    main()