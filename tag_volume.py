# -*- encoding:utf-8 -*-

import boto3
import config as cfg
import bill_cli as cli


class AWS_Tag_Volume(object):

    def __init__(self,config,commandline):

        self.config = config
        self.cli = commandline

        self.session = config.session
        self.session = boto3.Session(profile_name="dianda")
        self.ec2_client = self.session.client("ec2")
        self.ec2_resource = self.session.resource('ec2')

    def tag_volumes(self,profile):

        self.session = boto3.Session(profile_name=profile)
        self.ec2_client = self.session.client("ec2")
        self.ec2_resource = self.session.resource('ec2')


        response = self.ec2_client.describe_volumes()
        volumes = response["Volumes"]
        for volume in volumes:
            vp = ip = None
            #print volume
            #print volume["VolumeId"]
            v_id = volume["VolumeId"]
            if len(volume["Attachments"]) > 0:
                #print volume["Attachments"][0]["InstanceId"]
                instance_id = volume["Attachments"][0]["InstanceId"]
                instance = self.ec2_resource.Instance(instance_id)
                tags = instance.tags
                for tag in tags:
                    if tag["Key"] == "Project":
                        #print tag["Key"],tag["Value"]
                        ip = tag["Value"]
            else:
                self.cli.msg( "NULL Volume: "+v_id)

            if "Tags" in volume:
                for tag in volume["Tags"]:
                    if tag["Key"] == "Project":
                        #print tag["Key"], tag["Value"]
                        vp = tag["Value"]
            if ip != None and ip != vp:
                v = self.ec2_resource.Volume(v_id)
                v.create_tags(DryRun=False,
                            Tags=[
                                {
                                    'Key': 'Project',
                                    'Value': ip
                                },
                            ])
                self.cli.msg(v_id+": user:Project = "+ip)




def main():
    commandline = cli.CommandLine()
    commandline.get_options()
    commandline.set_options(profile="dianda")
    config = cfg.Config(commandline.option)

    aws_tag_volume = AWS_Tag_Volume(config=config,commandline=commandline)

    aws_tag_volume.tag_volumes(profile="dianda")
    aws_tag_volume.tag_volumes(profile="bill")

    """
    response = aws_tag_volume.ec2_client.describe_volumes()
    volumes = response["Volumes"]
    for volume in volumes:
        vp = ip = None
        #print volume
        #print volume["VolumeId"]
        v_id = volume["VolumeId"]
        if len(volume["Attachments"]) > 0:
            #print volume["Attachments"][0]["InstanceId"]
            instance_id = volume["Attachments"][0]["InstanceId"]
            instance = aws_tag_volume.ec2_resource.Instance(instance_id)
            tags = instance.tags
            for tag in tags:
                if tag["Key"] == "Project":
                    #print tag["Key"],tag["Value"]
                    ip = tag["Value"]
        else:
            print "NULL Volume"

        if "Tags" in volume:
            for tag in tags:
                if tag["Key"] == "Project":
                    #print tag["Key"], tag["Value"]
                    vp = tag["Value"]
        if ip != None and ip != vp:
            v = aws_tag_volume.ec2_resource.Volume(v_id)
            v.create_tags(DryRun=False,
                        Tags=[
                            {
                                'Key': 'Project',
                                'Value': ip
                            },
                        ])
    """

if __name__ == "__main__":
    main()