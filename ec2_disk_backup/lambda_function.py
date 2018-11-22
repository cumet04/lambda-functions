import boto3
from botocore.exceptions import ClientError

TARGET_TAG = 'backup_generation'
EC2_CLIENT = boto3.client('ec2')


def lambda_handler(event, context):
    for target in get_target_instances():
        if target['backup_gen'] < 1:
            continue
        for volume_id in target['disks']:
            desc = "Auto Snapshot for %s, %s" % (target['name'], volume_id)
            gen = target['backup_gen']
            EC2_CLIENT.create_snapshot(VolumeId=volume_id, Description=desc)

            snapshots = EC2_CLIENT.describe_snapshots(Filters=[{
                'Name': 'description',
                'Values': [desc]
            }])['Snapshots']

            for target in sorted(snapshots, key=lambda s: s['StartTime'])[:-gen]:
                EC2_CLIENT.delete_snapshot(SnapshotId=target['SnapshotId'])


def get_target_instances():
    reservations = EC2_CLIENT.describe_instances(Filters=[{
        'Name': 'tag-key',
        'Values': [TARGET_TAG]
    }])['Reservations']

    res = []
    for reservation in reservations:
        tags = get_tags(reservation)
        blocks = reservation['Instances'][0]['BlockDeviceMappings']
        res.append({
            'name': tags['Name'],
            'backup_gen': int(tags[TARGET_TAG]),
            'disks': [d['Ebs']['VolumeId'] for d in blocks]
        })
    return res


def get_tags(reservation):
    res = {}
    for raw_tag in reservation['Instances'][0]['Tags']:
        res[raw_tag['Key']] = raw_tag['Value']
    return res
