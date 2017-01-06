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

        #self.session = config.session
        self.session = boto3.Session(profile_name="dianda")
        self.ec2_client = self.session.client("ec2")
        self.ec2_resource = self.session.resource('ec2')


    def con_redshfit(self):
        conn = pg.connect(dbname=self.config.redshift_db, \
                               host=self.config.redshift_host, \
                               port=self.config.redshift_port, \
                               user=self.config.redshift_user, \
                               password=self.config.redshift_pwd)

        cur = conn.cursor()

        return conn, cur

    def discon_redshfit(self,conn,cur):

        cur.close()
        conn.close()


    def upload_to_stat(self,file,name):

        file_data = open(file, 'rb')

        # init aws_session
        session = boto3.Session(profile_name="edusoft")
        resource = session.resource('s3')
        s3key = self.config.stat_folder + name
        file_obj = resource.Bucket( \
            self.config.proc_bucket).put_object(Key=s3key, Body=file_data)

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
                    fromport = int(ipp["FromPort"])
                    toport = int(ipp["ToPort"])
                    if fromport<=80 and toport>=80:
                        port = "80"
                        break;
                    elif fromport<=433 and toport>=433:
                        port = "433"

                else:
                    port = "all"
                    break;

            self.df_sg.loc[len(self.df_sg)] = [GroupId,port]

        filename = "sg-" + profile + ".csv"
        file = os.path.join(self.config.stat_dir, filename)
        self.df_sg.to_csv(file, index=False, quoting=csv.QUOTE_ALL)

        self.upload_to_stat(file=file,name=filename)

    def get_EIPs(self, profile):
        session = boto3.Session(profile_name=profile)
        client = session.client("ec2")

        response = client.describe_addresses()

        list = response["Addresses"]
        self.df_eip = pandas.DataFrame(columns=["PublicIp", "AllocationId","InstanceId"])
        for item in list:
            PublicIp = item["PublicIp"]
            AllocationId = item["AllocationId"]
            InstanceId = "Null"
            if item.has_key("InstanceId"):
                InstanceId = item["InstanceId"]

            self.df_eip.loc[len(self.df_eip)] = [PublicIp,AllocationId, InstanceId]

        filename = "eips-" + profile + ".csv"
        file = os.path.join(self.config.stat_dir, filename)
        self.df_eip.to_csv(file, index=False, quoting=csv.QUOTE_ALL)

        self.upload_to_stat(file=file, name= filename)


    def get_elbs(self, profile):
        session = boto3.Session(profile_name=profile)
        client = session.client("elb")

        response = client.describe_load_balancers()


        self.ELBs = response["LoadBalancerDescriptions"]
        self.df_elb = pandas.DataFrame(columns=["LoadBalancerName", \
                                                "InstanceId", \
                                                "LoadBalancerPort"])
        for elb in self.ELBs:
            LoadBalancerName = elb["LoadBalancerName"]
            ListenerDescriptions = elb["ListenerDescriptions"]
            LoadBalancerPort = ""
            for item in ListenerDescriptions:
                Listener = item["Listener"]
                if Listener["LoadBalancerPort"] == 80 or \
                    Listener["LoadBalancerPort"] == 8080 or \
                    Listener["LoadBalancerPort"] == 433:
                    LoadBalancerPort = "web"

            Instances = elb["Instances"]
            for Instance in Instances:
                InstanceId = Instance["InstanceId"]

                self.df_elb.loc[len(self.df_elb)] = [LoadBalancerName, \
                                                     InstanceId,LoadBalancerPort]

        filename = "elbs-" + profile + ".csv"
        file = os.path.join(self.config.stat_dir, filename)

        self.df_elb.to_csv(file, index=False, quoting=csv.QUOTE_ALL)

        self.upload_to_stat(file=file, name=filename)

    def get_ec2_instances(self,profile):
        self.session = boto3.Session(profile_name=profile)
        self.ec2_client = self.session.client("ec2")
        self.ec2_resource = self.session.resource('ec2')

        response = self.ec2_client.describe_instances()

        self.instances_reservations = response["Reservations"]

        self.df_ins = pandas.DataFrame(columns=["InstanceId", "State",\
                                                "Project","Name","PublicIp",\
                                                "GroupName","GroupId",])

        for r in self.instances_reservations:
            instances = r["Instances"]
            for i in instances:
                InstanceId = i["InstanceId"]
                state = i["State"]["Name"]
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
                    self.df_ins.loc[len(self.df_ins)] = [InstanceId, state,\
                                                         project, name, pip,\
                                                         sg["GroupName"], sg["GroupId"]]
        filename = "ec2-" + profile + ".csv"
        file = os.path.join(self.config.stat_dir, filename)

        self.df_ins.to_csv(file, index=False, quoting=csv.QUOTE_ALL)
        self.upload_to_stat(file=file, name=filename)

    def check_instance(self,profile):

        self.get_security_groups(profile=profile)
        self.get_ec2_instances(profile=profile)

        self.get_elbs(profile=profile)

        self.get_EIPs(profile=profile)

        data1  = pandas.merge(left=self.df_ins,right=self.df_sg, how="left", \
                                on="GroupId")

        data2 = pandas.merge(left=data1, right=self.df_elb, how="left", \
                             on="InstanceId")

        data = pandas.merge(left=data2, right=self.df_eip[["PublicIp", "AllocationId"]], how="left", \
                            on="PublicIp")

        filename = self.config.instances_prefix+profile+".csv"
        file = os.path.join(self.config.stat_dir, filename)

        data[["InstanceId","State","Project","Name","PublicIp",\
              "GroupName","Port","LoadBalancerName","AllocationId"] \
            ].to_csv(file, index=False, quoting=csv.QUOTE_ALL)

        file_data = open(file, 'rb')

        # init aws_session
        self.session = boto3.Session(profile_name="edusoft")
        self.s3_resource = self.session.resource('s3')
        s3key = self.config.stat_folder+filename
        file_obj = self.s3_resource.Bucket( \
            self.config.proc_bucket).put_object(Key=s3key, Body=file_data)

    def load_table(self,table):
        conn , cur = self.con_redshfit()

        sql = "DELETE "+table+";"
        cur.execute(sql)
        conn.commit()

        old_isolation_level = conn.isolation_level
        conn.set_isolation_level(0)

        sql = "VACUUM FULL "+table+";"
        cur.execute(sql)
        conn.commit()

        conn.set_isolation_level(old_isolation_level)

        sql = "copy "+table+" from 's3://edusoft-aws-bill-work/stat/"+table + \
                        "' credentials 'aws_access_key_id=AKIAPIIVJSNYQ2S2YNDA;aws_secret_access_key=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' \
                        delimiter ',' removequotes  region 'cn-north-1' ignoreheader 1;"

        print sql
        cur.execute(sql)
        conn.commit()



        self.discon_redshfit(conn=conn,cur=cur)

    def load_into_redshift(self):

        self.load_table("instances")
        self.load_table("eips")
        self.load_table("elbs")

        """

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
        """

def main():

    commandline = cli.CommandLine()
    commandline.get_options()

    commandline.set_options(profile="dianda")
    config = cfg.Config(commandline.option)

    aws = AWS_Check_Instance(config=config, commandline=commandline)

    aws.check_instance(profile="edusoft")
    aws.check_instance(profile="dianda")

    aws.load_into_redshift()

if __name__ == "__main__":

    main()
