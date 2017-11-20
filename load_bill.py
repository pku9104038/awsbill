# -*- coding:utf-8 -*-
#
# load calc bills into redshift

import config as cfg
import bill_cli
import psycopg2 as pg
import json

class AWS_Load_Bill(object):
    """

    """


    def __init__(self, config, commandline):

        self.config = config
        self.cli = commandline

        self.conn  = self.cur = None

        self.session = config.session
        self.s3_client = config.s3_client
        self.s3_resource = config.s3_resource

    def con_redshfit(self):
        self.conn = pg.connect(dbname=self.config.redshift_db, \
                          host=self.config.redshift_host, \
                          port=self.config.redshift_port, \
                          user=self.config.redshift_user, \
                          password=self.config.redshift_pwd)

        self.cur = self.conn.cursor()


    def discon_redshfit(self):

        self.cur.close()
        self.conn.close()

    def single_quote(self,str):

        return "'"+str+"'"

    def sql_append(self, sql ="", key="", value=""):

        return sql + key + " "  + value + " "

    def sql_end(self,sql = ""):
        return sql+";"


    def table_copy(self, table, s3key, \
                   credentials = None, \
                   delimeter = ",", \
                   removequotes = "",\
                   ignoreheader = "1" , \
                   region="cn-north-1"):

        if credentials == None:
            credentials = self.config.s3_credentials

        self.con_redshfit()

        sql = self.sql_append(key="COPY", value=table)
        sql = self.sql_append(sql=sql, key="FROM", value=self.single_quote(s3key))
        sql = self.sql_append(sql=sql, key="credentials", value=self.single_quote(credentials))
        sql = self.sql_append(sql=sql, key="delimiter", value=self.single_quote(delimeter))
        sql = self.sql_append(sql=sql, key="removequotes", value=removequotes)

        sql = self.sql_append(sql=sql, key="ignoreheader", value=ignoreheader)
        sql = self.sql_append(sql=sql, key="region", value=self.single_quote(region))
        sql = self.sql_end(sql=sql)

        self.cli.msg(sql)
        self.cur.execute(sql)

        self.conn.commit()

        self.discon_redshfit()


    def table_del(self, table):
        self.con_redshfit()

        sql = self.sql_append(key="DELETE", value=table)
        sql = self.sql_end(sql=sql)

        self.cli.msg(sql)
        self.cur.execute(sql)
        self.conn.commit()

        old_isolation_level = self.conn.isolation_level
        self.conn.set_isolation_level(0)

        sql = self.sql_append(key="VACUUM FULL", value=table)
        sql = self.sql_end(sql=sql)

        self.cli.msg(sql)
        self.cur.execute(sql)
        self.conn.commit()

        self.conn.set_isolation_level(old_isolation_level)

        self.discon_redshfit()


    def table_vacuum(self, table):
        self.con_redshfit()

        old_isolation_level = self.conn.isolation_level
        self.conn.set_isolation_level(0)

        sql = self.sql_append(key="VACUUM FULL", value=table)
        sql = self.sql_end(sql=sql)

        self.cli.msg(sql)
        self.cur.execute(sql)
        self.conn.commit()

        self.conn.set_isolation_level(old_isolation_level)

        self.discon_redshfit()

    def delete_estimated_rows(self, table, where):

        self.con_redshfit()

        sql = self.sql_append(key="DELETE FROM", value=table)
        sql = self.sql_append(sql=sql,key="WHERE", value=table+"."+where)
        sql = self.sql_end(sql=sql)

        self.cli.msg(sql)
        self.cur.execute(sql)
        self.conn.commit()

        old_isolation_level = self.conn.isolation_level
        self.conn.set_isolation_level(0)

        sql = self.sql_append(key="VACUUM FULL", value=table)
        sql = self.sql_end(sql=sql)

        self.cli.msg(sql)
        self.cur.execute(sql)
        self.conn.commit()

        self.conn.set_isolation_level(old_isolation_level)

        self.discon_redshfit()

    def delete_month_rows(self, table, month):

        where = "billcycle = '" + month +"'"

        self.con_redshfit()

        sql = self.sql_append(key="DELETE FROM", value=table)
        sql = self.sql_append(sql=sql, key="WHERE", value=table + "." + where)
        sql = self.sql_end(sql=sql)

        self.cli.msg(sql)
        self.cur.execute(sql)
        self.conn.commit()

        old_isolation_level = self.conn.isolation_level
        self.conn.set_isolation_level(0)

        sql = self.sql_append(key="VACUUM FULL", value=table)
        sql = self.sql_end(sql=sql)

        self.cli.msg(sql)
        self.cur.execute(sql)
        self.conn.commit()

        self.conn.set_isolation_level(old_isolation_level)

        self.discon_redshfit()

    def check_new_calc_bill(self):
        """

        :param
        :return:
        """
        # item detail logs object list
        objs = self.s3_client.list_objects(Bucket=self.config.proc_bucket, \
                                    Prefix= self.config.cal_folder+self.config.estimated_prefix)["Contents"]

        for obj in objs:
            bucket = self.s3_resource.Bucket(self.config.proc_bucket)
            csvname = obj["Key"][len(self.config.cal_folder+self.config.estimated_prefix):]
            key = self.config.cal_folder + self.config.cal_prefix+csvname
            #key = "calc/calc-2016-10.csv"
            csvs = list(bucket.objects.filter(Prefix=key))

            if len(csvs)>0 and csvs[0].key == key:
                return key, obj["Key"]


        return None, None

    def check_month_calc_bill(self,month):
        """

        :param
        :return:
        """

        estimated = None
        calc = None
        bucket = self.s3_resource.Bucket(self.config.proc_bucket)

        key = self.config.cal_folder + \
                        self.config.estimated_prefix + \
                        month + ".csv"
        obj = list(bucket.objects.filter(Prefix=key))
        if len(obj) > 0 and obj[0].key == key:
            estimated = key

        key = self.config.cal_folder + \
                  self.config.cal_prefix + \
                  month + ".csv"
        obj = list(bucket.objects.filter(Prefix=key))
        if len(obj) > 0 and obj[0].key == key:
            calc = key

        if calc != None:
            if estimated != None:
                (self.s3_resource.Object(self.config.proc_bucket, \
                                         estimated)).delete()
                estimated = None

        return calc, estimated

    def update_bill_datetime(self):

        #self.con_redshfit()

        #sql = "delete bill_datetime;"
        #self.cli.msg(sql)
        #self.cur.execute(sql)
        #self.conn.commit()

        self.table_del(table="bill_datetime")

        self.con_redshfit()

        sql = "insert into bill_datetime select payeraccountid, billstop \
                from history_bill where billstop = (select max(billstop) from history_bill ) limit 1;"

        self.cli.msg(sql)
        self.cur.execute(sql)
        self.conn.commit()

        self.discon_redshfit()

    def load_latest_bills(self):
        """

        :return:
        """

        self.cli.msg("LOAD_BILL: ")
        #print self.config.month_list
        self.cli.msg(json.dumps(obj=self.config.month_list, indent=4))
        calc_bill, estimated_bill = self.check_new_calc_bill()
        if (calc_bill!=None):

            object = self.s3_resource.Object(self.config.proc_bucket,\
                                             estimated_bill)
            self.cli.msg("Delete Estimated CSV: "+estimated_bill)
            object.delete()
            #self.cli.msg("Delete: " + self.config.redshift_t_month)
            #self.table_del(table=self.config.redshift_t_month)
            #self.cli.msg("Copy into Redshift: " + calc_bill)
            #self.table_copy(table=self.config.redshift_t_month,\
            #                s3key=calc_bill)
            self.table_copy(table=self.config.redshift_t_history,\
                            s3key="s3://"+self.config.proc_bucket+"/"+ \
                                  calc_bill)


        #self.cli.msg("Delete: " + self.config.redshift_t_estimated)
        self.table_del(table=self.config.redshift_t_estimated)

        #self.cli.msg("Copy : estimated bills")
        self.table_copy(table=self.config.redshift_t_estimated, \
                            s3key="s3://"+self.config.proc_bucket+"/"+ \
                                  self.config.cal_folder+self.config.estimated_prefix \
                         #+"2016-12.csv"\
                        )

        self.delete_estimated_rows(table=self.config.redshift_t_history,\
                                   where="invoiceid='Estimated'")

        self.table_copy(table=self.config.redshift_t_history, \
                        s3key="s3://" + self.config.proc_bucket + "/" + \
                              self.config.cal_folder + self.config.estimated_prefix)
        #print "\n"

        self.update_bill_datetime()


    def load_all_bills(self):
        """

        :param self:
        :return:
        """

        # delete data in estimated table then copy estimated csv into it
        self.table_del(table=self.config.redshift_t_estimated)
        self.table_copy(table=self.config.redshift_t_estimated, \
                        s3key="s3://" + self.config.proc_bucket + "/" + \
                              self.config.cal_folder + self.config.estimated_prefix \
                        )

        # delete data in history table then copy all calc/ csv into it
        self.table_del(table=self.config.redshift_t_history)
        self.table_copy(table=self.config.redshift_t_history, \
                        s3key="s3://" + self.config.proc_bucket + "/" + \
                              self.config.cal_folder)

        self.update_bill_datetime()

    def load_month_bill(self,month):
        """

        :return:
        """
        calc, estimated = self.check_month_calc_bill(month=month)
        if calc != None:
            self.delete_estimated_rows(table=self.config.redshift_t_history, \
                               where="billcycle='" + month +"'")

            self.table_copy(table=self.config.redshift_t_history, \
                            s3key="s3://" + self.config.proc_bucket + "/" +calc)

        if estimated != None:
            self.delete_estimated_rows(table=self.config.redshift_t_estimated, \
                                       where="billcycle='" + month + "'")
            self.table_copy(table=self.config.redshift_t_estimated, \
                            s3key="s3://" + self.config.proc_bucket + "/" + estimated)


            self.delete_estimated_rows(table=self.config.redshift_t_history, \
                                       where="billcycle='" + month + "'")
            self.table_copy(table=self.config.redshift_t_history, \
                s3key="s3://" + self.config.proc_bucket + "/" + estimated)

        self.update_bill_datetime()



    def load_bills(self):

        if self.config.scope == "all":
            self.load_all_bills()
        elif self.config.scope == "latest" or self.config.scope == "last":
            self.load_latest_bills()
        else:
            self.load_month_bill(self.config.scope)

        self.update_bill_datetime()



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
    aws_load_bill = AWS_Load_Bill(config=config, commandline = cli)

    month = config.end_month
    aws_load_bill.delete_month_rows(table=aws_load_bill.config.redshift_t_history, \
                               month=month)
    calc_key = "s3://" + aws_load_bill.config.proc_bucket + "/" + \
                      aws_load_bill.config.cal_folder + \
                      aws_load_bill.config.cal_prefix +month
    est_key = "s3://" + aws_load_bill.config.proc_bucket + "/" + \
                      aws_load_bill.config.cal_folder + \
                      aws_load_bill.config.estimated_prefix +month
    try:
        aws_load_bill.table_copy(table=aws_load_bill.config.redshift_t_history, \
                s3key="s3://" + aws_load_bill.config.proc_bucket + "/" + \
                      aws_load_bill.config.cal_folder + \
                      aws_load_bill.config.cal_prefix +month)
    except Exception as err:
        print(err)
        aws_load_bill.table_copy(table=aws_load_bill.config.redshift_t_history, \
                             s3key="s3://" + aws_load_bill.config.proc_bucket + "/" + \
                                   aws_load_bill.config.cal_folder + \
                                   aws_load_bill.config.estimated_prefix + month)
    finally:
        #print "\n"
        aws_load_bill.cli.msg("You got it !  Cheers!")


if __name__ == '__main__':
    main()
