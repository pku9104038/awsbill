# -*- coding:utf-8 -*-

import bill_cli


def main():
    """
    main function for this script
    :return:
    """

    # get options
    cli = bill_cli.CommandLine()

    cli.get_options()

    cli.msg("I am here")

if __name__ == '__main__':
    main()