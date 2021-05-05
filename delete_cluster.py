import boto3
import configparser
import json

# Load DWH Params
config = configparser.ConfigParser()
config.read_file(open("dwh.cfg"))
KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")


redshift = boto3.client(
    "redshift",
    region_name="us-west-2",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

iam = boto3.client(
    "iam",
    region_name="us-west-2",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

# clusters_metadata = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
# print(clusters_metadata)
# cluster_status = clusters_metadata.get("Clusters", [])[0].get("ClusterStatus")
# print(f"Cluster Status: {cluster_status}")


def delete_cluster(cluster_name=DWH_CLUSTER_IDENTIFIER):
    try:
        print(f"Deleting cluster {cluster_name}...\n")
        redshift.delete_cluster(
            ClusterIdentifier=cluster_name,
            SkipFinalClusterSnapshot=True,
        )
    except Exception as e:
        print(e)


def detach_role_policy(
    role_name=DWH_IAM_ROLE_NAME,
    policy_arn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
):
    try:
        print(f"Detaching role {role_name}...\n")
        iam.detach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn,
        )
    except Exception as e:
        print(e)


def delete_role(role_name=DWH_IAM_ROLE_NAME):
    try:
        print(f"Deleting role {role_name}...\n")
        iam.delete_role(RoleName=role_name)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    print("Starting deletions...\n")
    delete_cluster()
    detach_role_policy()
    delete_role()
