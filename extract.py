# -*- coding: utf-8 -*-

import sys
import os
import bill



def main():
    reload(sys)
    sys.setdefaultencoding('utf8')
    curdir = os.path.abspath(os.path.curdir)

    procenv = bill.extract.ProcessingEnv(curdir)



    object_summary_iterator = procenv.bill_bucket.objects.all()
    for obj_i in object_summary_iterator:
        if obj_i.key.find(procenv.bills_prefix) > -1:
            print obj_i.key



if __name__ == '__main__':
    main()