# -*- eccoding:utf-8  -*-

import pandas
import boto3
import bill_cli as cli
import config as cfg
import csv, os

class AWS_Des_Instances(object):

    def __init__(self,config,commandline):

        self.config = config
        self.cli = commandline

    def save_to_stat(self, profile_data, dataframe, prefix, profile_s3):

        filename = prefix + profile_data + ".csv"
        file = os.path.join(self.config.stat_dir, filename)
        dataframe.to_csv(file, index=False, quoting=csv.QUOTE_ALL)

        file_data = open(file, 'rb')
        # init aws_session
        session = boto3.Session(profile_name=profile_s3)
        resource = session.resource('s3')
        s3key = self.config.stat_folder + filename
        file_obj = resource.Bucket( \
            self.config.proc_bucket).put_object(Key=s3key, Body=file_data)


    def get_reserved_instances(self,profile):

        session = boto3.Session(profile_name=profile)
        client = session.client("ec2")

        response = client.describe_reserved_instances()

        reserved_instances = response["ReservedInstances"]

        dataframe = pandas.DataFrame(columns=["ProductDescription", "InstanceType", \
                                                "State", "InstanceCount"])

        for r in reserved_instances:
            ProductDescription = r["ProductDescription"]
            InstanceType = r["InstanceType"]
            State = r["State"]
            InstanceCount = r["InstanceCount"]

            dataframe.loc[len(dataframe)] = [ProductDescription, \
                                               InstanceType, \
                                               State, InstanceCount]

        #self.save_to_stat(profile_data=profile, dataframe=dataframe,\
        #                  prefix="ri-", profile_s3="edusoft")


        return dataframe



    def set_platform(self,row):
        if row["Windows"] == "Windows":
            return "Windows"
        else:
            return "Linux/UNIX"


    def get_running_instances(self, profile):

        session = boto3.Session(profile_name=profile)
        client = session.client("ec2")

        Filters = [
            {
                'Name': 'platform',
                'Values': [
                    'windows',
                ]
            },
        ]
        response = client.describe_instances(Filters=Filters)

        running_instances = response["Reservations"]

        dataframe = pandas.DataFrame(columns=["InstanceId", "Windows"])

        for r in running_instances:
            instance = r["Instances"]
            for i in instance:
                InstanceId = i["InstanceId"]
                platform = "Windows"
                dataframe.loc[len(dataframe)] = [InstanceId, \
                                                   platform]



        response = client.describe_instances()

        running_instances = response["Reservations"]

        dataframe1 = pandas.DataFrame(columns=["InstanceId", "InstanceType", \
                                              "State", "Linux"])

        for r in running_instances:
            instance = r["Instances"]
            for i in instance:
                InstanceId = i["InstanceId"]
                InstanceType = i["InstanceType"]
                State = i["State"]["Name"]
                platform = "Linux"
                dataframe1.loc[len(dataframe1)] = [InstanceId, \
                                                 InstanceType, \
                                                 State, platform]

        data = pandas.merge(left=dataframe1, right=dataframe, how="left", \
                                 on="InstanceId")

        data["Platform"] = data.apply(self.set_platform,axis=1)
        data.loc[:,"InstanceCount"] = 1

        ru_data = data[["InstanceId",\
            "InstanceType","State","Platform","InstanceCount"]]

        #self.save_to_stat(profile_data=profile, dataframe=ru_data, \
        #                  prefix="ru-", profile_s3="edusoft")

        return ru_data

    def des_instances(self,profile):

        ri_data_dianda = self.get_reserved_instances(profile="dianda")
        ri_data_edusoft = self.get_reserved_instances(profile="edusoft")
        ri_data = pandas.concat([ri_data_dianda,ri_data_edusoft])

        ri_data = ri_data[ri_data["State"]=="active"]

        grouped = ri_data.groupby(["ProductDescription","InstanceType"],axis=0)
        data_ri = pandas.DataFrame(columns=["Platform", "InstanceType", "ReservedCount"])
        for name, group in grouped:
            data_ri.loc[len(data_ri)] = [name[0], \
                                               name[1], \
                                               group["InstanceCount"].sum()]


        #self.save_to_stat(profile_data="all", dataframe=data_ri, \
        #                  prefix="ri-", profile_s3="edusoft")

        ru_data_dianda = self.get_running_instances(profile="dianda")
        ru_data_edusoft = self.get_running_instances(profile="edusoft")

        ru_data = pandas.concat([ru_data_dianda,ru_data_edusoft])


        ru_data = ru_data[ru_data["State"]=="running"]

        grouped = ru_data.groupby(["Platform", "InstanceType"], axis=0)
        data_ru = pandas.DataFrame(columns=["Platform", "InstanceType", "RunningCount"])
        for name, group in grouped:
            data_ru.loc[len(data_ru)] = [name[0], \
                                         name[1], \
                                         group["InstanceCount"].sum()]

        #self.save_to_stat(profile_data="all", dataframe=data_ru, \
        #                   prefix="ru-", profile_s3="edusoft")

        data = pandas.merge(data_ru, data_ri, how='outer', on=['Platform', 'InstanceType'])
        data.loc[:,"ProductName"] = "EC2"

        data = data[["ProductName","Platform","InstanceType","RunningCount","ReservedCount"]]

        self.save_to_stat(profile_data="ec2", dataframe=data, \
                          prefix="ri-", profile_s3="edusoft")


def main():
    commandline = cli.CommandLine()
    commandline.get_options()

    commandline.set_options(profile="dianda")
    config = cfg.Config(commandline.option)

    aws = AWS_Des_Instances(config=config, commandline=commandline)

    aws.des_instances(profile="edusoft")
    #aws.get_running_instances(profile="dianda")

    #aws.load_into_redshift()

if __name__ == "__main__":
    main()