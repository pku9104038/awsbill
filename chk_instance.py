#  -*- encoding: utf-8 -*-

import boto3
import config as cfg
import bill_cli as cli
import psycopg2 as pg
import pandas
import csv
import os

class AWS_Check_Instance(object):

    def __init__(self, config, commandline):
        self.config = config
        self.cli = commandline

        self.session = config.session
        self.session = boto3.Session(profile_name="dianda")
        self.ec2_client = self.session.client("ec2")
        self.ec2_resource = self.session.resource('ec2')


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

    def get_security_groups(self,profile):
        self.session = boto3.Session(profile_name=profile)
        self.ec2_client = self.session.client("ec2")

        response = self.ec2_client.describe_security_groups()

        self.SecurityGroups = response["SecurityGroups"]
        self.df_sg = pandas.DataFrame(columns=["GroupId","Port"])
        for sg in self.SecurityGroups:
            GroupId = sg["GroupId"]
            IpPermissions = sg["IpPermissions"]
            port = ""
            for ipp in IpPermissions:
                port = ""
                if ipp.has_key("FromPort") and ipp.has_key("ToPort"):
                    if port != "80":
                        fromport = int(ipp["FromPort"])
                        toport = int(ipp["ToPort"])
                        if fromport<=80 and toport>=80:
                            port = "80"
                        elif fromport<=433 and toport>=433:
                            port = "433"

                else:
                    port = "all"
                    break;

            self.df_sg.loc[len(self.df_sg)] = [GroupId,port]


        #print self.dfsg



    def get_ec2_instances(self,profile):
        self.session = boto3.Session(profile_name=profile)
        self.ec2_client = self.session.client("ec2")
        self.ec2_resource = self.session.resource('ec2')

        response = self.ec2_client.describe_instances()

        self.instances_reservations = response["Reservations"]

        self.df_ins = pandas.DataFrame(columns=["InstanceId", "Project","Name","PublicIp","GroupName","GroupId"])

        for r in self.instances_reservations:
            instances = r["Instances"]
            for i in instances:
                InstanceId = i["InstanceId"]
                NetworkInterfaces = i["NetworkInterfaces"]
                pip = ""
                for nif in NetworkInterfaces:
                    if nif.has_key("Association"):
                        Association = nif["Association"]
                        if Association.has_key("PublicIp"):
                            pip = Association["PublicIp"]
                            break

                name = ""
                project = ""
                if i.has_key("Tags"):
                    Tags = i["Tags"]
                    for tag in Tags:
                        if tag["Key"] == "Name":
                            name = tag["Value"]
                        if tag["Key"] == "Project":
                            project = tag["Value"]

                SecurityGroups = i["SecurityGroups"]

                for sg in SecurityGroups:
                    self.df_ins.loc[len(self.df_ins)] = [InstanceId, project, name, pip,sg["GroupName"], sg["GroupId"]]

        #print self.df_ins

    def check_instance(self,profile):

        self.get_security_groups(profile=profile)
        self.get_ec2_instances(profile=profile)

        data  = pandas.merge(left=self.df_ins,right=self.df_sg, how="left", \
                                on="GroupId")
        filename = self.config.instances_prefix+profile+".csv"
        file = os.path.join(self.config.stat_dir, filename)

        data[["InstanceId","Project","Name","PublicIp","GroupName","Port"]].to_csv(file, index=False, quoting=csv.QUOTE_ALL)
        file_data = open(file, 'rb')

        # init aws_session
        self.session = boto3.Session(profile_name="edusoft")
        self.s3_resource = self.session.resource('s3')
        s3key = self.config.stat_folder+filename
        file_obj = self.s3_resource.Bucket( \
            self.config.proc_bucket).put_object(Key=s3key, Body=file_data)

    def load_into_redshift(self):

        self.con_redshfit()

        sql = "DELETE instances;"
        self.cur.execute(sql)
        self.conn.commit()

        old_isolation_level = self.conn.isolation_level
        self.conn.set_isolation_level(0)

        sql = "VACUUM FULL instances;"
        self.cur.execute(sql)
        self.conn.commit()

        self.conn.set_isolation_level(old_isolation_level)

        sql = "copy instances from 's3://edusoft-aws-bill-work/stat/instance' \
                credentials 'aws_access_key_id=AKIAPIIVJSNYQ2S2YNDA;aws_secret_access_key=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' \
                delimiter ',' removequotes  region 'cn-north-1' ignoreheader 1;"

        self.cur.execute(sql)
        self.conn.commit()


        self.discon_redshfit()

def main():

    commandline = cli.CommandLine()
    commandline.get_options()
    commandline.set_options(profile="dianda")
    config = cfg.Config(commandline.option)

    aws = AWS_Check_Instance(config=config, commandline=commandline)

    aws.check_instance(profile="dianda")
    aws.check_instance(profile="edusoft")

    aws.load_into_redshift()

if __name__ == "__main__":

    main()
