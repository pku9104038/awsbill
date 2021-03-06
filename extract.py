# -*- coding: utf-8 -*-

import sys
import os
from bill import extract



def main():
    reload(sys)
    sys.setdefaultencoding('utf8')
    curdir = os.path.abspath(os.path.curdir)

    procenv = extract.ProcessingEnv(curdir)



    object_summary_iterator = procenv.bill_bucket.objects.all()
    for obj_i in object_summary_iterator:
        if obj_i.key.find(procenv.bills_prefix) > -1:
            print obj_i.key
            extract.extractZipBill(procenv, obj_i.bucket_name, obj_i.key)


if __name__ == '__main__':
    main()