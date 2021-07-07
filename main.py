import argparse
from typing import Any, Iterator, List, Set, Tuple

import boto3

cfn = boto3.client('cloudformation')
s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')
test_mode = False

def aws_tags_to_set(tags):
    """Convert a list of AWS Tag objects to a python set

    ### Example:
    ```python
    aws_tags = [{'Key': 'lime-energy:stack:env', 'Value': 'prod'}, {'Key': 'lime-energy:stack:name', 'Value': 'prod052021'}]
    aws_tags_to_set(aws_tags)
    # {'lime-energy:stack:name:prod052021', 'lime-energy:stack:env:prod'}
    ```
    """
    return {'{Key}:{Value}'.format(**x) for x in tags}

def tag_match(filter: Set[str], target: Set[str]):
    """For each item in the filter set, returns true if the item is in the target set

    ### Example:

    ```python
    filter = {'blue', 'orange', 'lime'}
    target = {'red', 'yellow', 'lime', 'salmon', 'blue' }

    list(tag_match(filter, target))
    # [True, False, True]
    ```
    """
    return (tag in target for tag in filter)


def get_stacks(tags_include: Set[str], *, next_token: str = None) -> Iterator[Any]:
    if next_token:
        res = cfn.describe_stacks(NextToken=next_token)
    else:
        res = cfn.describe_stacks()

    stacks = ( stack for stack in res.get('Stacks', [])
        if all(
            tag_match(
                tags_include,
                aws_tags_to_set( stack.get('Tags', []) )
            )
        )
    )

    token = res.get('NextToken')

    for stack in stacks:
        yield stack

    if token:
        yield from get_stacks(tags_include, next_token=token)

def get_s3_tags(bucket_name):
    cli = boto3.client('s3')
    res = cli.get_bucket_tagging(Bucket=bucket_name)
    tags = aws_tags_to_set(res.get('TagSet'))
    return tags

def get_dynamo_tags(table_name):
    def _get_resource_tags(arn, token=None):
        cli = boto3.client('dynamodb')
        res = cli.list_tags_of_resource(ResourceArn=arn)

        token = res.get('NextToken')
        tags = res.get('Tags')

        for tag in tags:
            yield tag

        if token:
            yield from _get_resource_tags(arn, token)

    arn = dynamodb.Table(table_name).table_arn
    tags = aws_tags_to_set(_get_resource_tags(arn))

    return tags

def get_resources(stack, tags_exclude: Set[str]) -> Tuple[List[str], List[str]]:
    res = cfn.describe_stack_resources(StackName=stack.get('StackName'))
    resources = res.get('StackResources', [])

    buckets = ( resource.get('PhysicalResourceId') for resource in resources
        if resource.get('ResourceType') == 'AWS::S3::Bucket' )

    buckets = ( bucket for bucket in buckets
        if not any(
            tag_match(
                tags_exclude,
                get_s3_tags(bucket)
            )
        )
    )

    dynamo_tables = ( resource.get('PhysicalResourceId') for resource in resources if resource.get('ResourceType') == 'AWS::DynamoDB::Table' )
    dynamo_tables = ( table for table in dynamo_tables
        if not any(
            tag_match(
                tags_exclude,
                get_dynamo_tags(table)
            )
        )
    )

    return (buckets, dynamo_tables)

def clear_buckets(buckets):
    for bucket_name in buckets:
        print('└─Bucket:', bucket_name)
        if not test_mode:
            bucket = s3.Bucket(bucket_name)
            bucket.objects.all().delete()

def clear_dynamo_tables(dynamo_tables):
    for table_name in dynamo_tables:
        print('└─Table:', table_name)
        if not test_mode:
            table = dynamodb.Table(table_name)
            primary_key = [x.get('AttributeName') for x in table.key_schema]
            scan = table.scan(AttributesToGet=primary_key)
            with table.batch_writer() as batch:
                for each in scan['Items']:
                    batch.delete_item(Key=each)

def run(tags_include: Set[str], tags_exclude: Set[str]):
    for stack in get_stacks(tags_include):
        print()
        print('######################################')
        print('Stack:', stack.get('StackName'))
        print('Resources:')

        (buckets, dynamo_tables) = get_resources(stack, tags_exclude)
        clear_buckets(buckets)
        clear_dynamo_tables(dynamo_tables)

def _str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def _get_args():
    parser = argparse.ArgumentParser(description=(
        'Script Empty Buckets And Dynamo'
    ))

    parser.add_argument('--tags', nargs='+', help='Param description', default='', required=True)
    parser.add_argument('--tags-exclude', nargs='+', help='Param description', default='', required=False)
    parser.add_argument("--dry-run", type=_str2bool, nargs='?', const=True, default=False)

    return parser.parse_args()


def main():
    args = _get_args()
    global test_mode
    test_mode = args.dry_run

    if test_mode:
        print('Running in dry-run mode - Nothing will be removed')
    else:
        print('Removing data from:')

    run(set(args.tags), set(args.tags_exclude))

if __name__ == '__main__':
    main()
