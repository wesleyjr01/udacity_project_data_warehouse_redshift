import boto3
import configparser
import json
import time


# Load DWH Params
config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))
KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")


# Create clients for EC2, S3, IAM and Redshift
region = "us-west-2"
ec2 = boto3.resource(
    "ec2",
    region_name=region,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

s3 = boto3.resource(
    "s3",
    region_name=region,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

iam = boto3.client(
    "iam",
    region_name=region,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

redshift = boto3.client(
    "redshift",
    region_name=region,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)


def create_iam_role(role_name=DWH_IAM_ROLE_NAME):
    try:
        print("1.1 Creating a new IAM Role...\n")
        iam.create_role(
            Path="/",
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            ),
            Description="Allows Redshift clusters to call AWS services on your behalf",
        )

    except Exception as e:
        print(e)


def attach_role_policy(role_name=DWH_IAM_ROLE_NAME):
    try:
        print(f"1.2 Attaching S3 Read Only Acess to role {role_name}... \n")
        iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
        )
    except Exception as e:
        print(e)


def create_redshift_cluster(role_name=DWH_IAM_ROLE_NAME):
    try:
        print(
            (
                "1.3 Creating Redshift Cluster with: \n"
                f"{DWH_NUM_NODES} nodes, of type {DWH_NODE_TYPE}\n"
            )
        )
        role_arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
        redshift.create_cluster(
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            NodeType=DWH_NODE_TYPE,
            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            # Roles (for s3 access)
            IamRoles=[role_arn],
        )
    except Exception as e:
        print(e)


def open_tcp_port_to_externally_acess_cluster(
    cluster_name=DWH_CLUSTER_IDENTIFIER,
    from_port=int(DWH_PORT),
    to_port=int(DWH_PORT),
    cidr_ip="0.0.0.0/0",
):
    print("1.4 Opening an incoming TPC port to acess the cluster endpoint externally.")
    try:
        metadata = redshift.describe_clusters(ClusterIdentifier=cluster_name)
        redshift_metadata = metadata.get("Clusters", "No Clusters Found")[0]
        vpc_id = redshift_metadata.get("VpcId", "No VpcId Found")

        vpc = ec2.Vpc(id=vpc_id)
        default_security_group = list(vpc.security_groups.all())[0]
        print(default_security_group)

        default_security_group.authorize_ingress(
            GroupName=default_security_group.group_name,
            CidrIp=cidr_ip,
            IpProtocol="TCP",
            FromPort=from_port,
            ToPort=to_port,
        )
    except Exception as e:
        print(e)


def retrieve_cluster_status(cluster_name=DWH_CLUSTER_IDENTIFIER):
    try:
        metadata = redshift.describe_clusters(ClusterIdentifier=cluster_name)
        redshift_metadata = metadata.get("Clusters", "No Clusters Found")[0]
        redshift_status = redshift_metadata.get("ClusterStatus", "No Status Found")
        return redshift_status
    except Exception as e:
        print(e)
        return "Error retrieving Redshift Cluster Status."


def check_cluster_status_repeatdly(seconds=20, status="creating"):
    """We will check in a loop a transition change on status, when a change
    in status happens, the loop will break."""
    redshift_status = retrieve_cluster_status()
    while redshift_status.lower() == status:
        print(f"Redshift Status: {redshift_status}")
        time.sleep(seconds)
        redshift_status = retrieve_cluster_status()
    print(f"Redshift Status: {redshift_status}")


def main():
    create_iam_role()
    attach_role_policy()
    create_redshift_cluster()
    open_tcp_port_to_externally_acess_cluster()
    check_cluster_status_repeatdly()


if __name__ == "__main__":
    main()
