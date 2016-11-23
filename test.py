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

    config = cfg.Config(scope=cli.scope)

    if config.scope != "all" and config.scope != "latest" \
            and config.scope != "last":
        now = time.localtime(time.time())
        yyyy_mm = str(now.tm_year) + "-" + str(now.tm_mon)
        start_month = config.scope
        config.scope = "all"
        config.month_list = config.get_months(start_yyyy_mm=start_month, \
                                      end_yyyy_mm=yyyy_mm)
        print config.month_list


if __name__ == '__main__':
    main()