import boto3

LOCALSTACK_ENDPOINT = 'http://localhost:4566'
STREAM_NAME = 'chat-messages'

kinesis = boto3.client(
    'kinesis',
    endpoint_url=LOCALSTACK_ENDPOINT,
    region_name='sa-east-1',
    aws_access_key_id='1234',
    aws_secret_access_key='1234'
)


def read_shard(shard_id):
    response = kinesis.get_shard_iterator(
        StreamName=STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType='TRIM_HORIZON'
    )

    records = kinesis.get_records(
        ShardIterator=response['ShardIterator']
    )['Records']

    return records


def list_shards():
    response = kinesis.list_shards(StreamName=STREAM_NAME)
    return response.get('Shards', [])


def read_stream():
    print(f'\nStream: {STREAM_NAME}')
    print('Searching for shards...')

    try:
        shards = list_shards()
    except kinesis.exceptions.ResourceNotFoundException:
        raise SystemExit(f'Stream {STREAM_NAME!r} was not found.')

    if not shards:
        raise SystemExit('The stream exists, but it does not have shards yet.')

    print(f'{len(shards)} shard(s) found.\n')

    stream_records = []
    for shard in shards:
        shard_id = shard['ShardId']  # e.g. shardId-000000000001 | shardId-000000000002
        records = read_shard(shard_id)

        for record in records:
            stream_records.append({
                'shard_id': shard_id,
                'record': record,
            })

    stream_records.sort(
        key=lambda item: item['record']['ApproximateArrivalTimestamp']
    )

    if not stream_records:
        print('No records found.')
    else:
        print('Records ordered by arrival time:\n')
        for item in stream_records:
            record = item['record']
            message = record['Data'].decode('utf-8')
            arrival_time = record['ApproximateArrivalTimestamp']
            print(f'[{item["shard_id"]}] {arrival_time.isoformat()}')
            print(f'  User: {record["PartitionKey"]}')
            print(f'  Message: {message}\n')

    print(
        f'Reading complete: {len(stream_records)} '
        'record(s) found.'
    )


if __name__ == '__main__':
    read_stream()
