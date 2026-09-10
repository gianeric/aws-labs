import boto3
import time

LOCALSTACK_ENDPOINT = 'http://localhost:4566'

kinesis = boto3.client(
    'kinesis',
    endpoint_url=LOCALSTACK_ENDPOINT,  # Use a string directly
    region_name='sa-east-1',
    aws_access_key_id='1234',
    aws_secret_access_key='1234'
)

# Create the stream if it does not already exist.
stream_name = 'chat-messages'
try:
    kinesis.create_stream(
        StreamName=stream_name,
        ShardCount=2
    )
except kinesis.exceptions.ResourceInUseException:
    pass

# Stream creation is asynchronous; writing is only allowed when the stream is ACTIVE.
for _ in range(30):
    status = kinesis.describe_stream_summary(
        StreamName=stream_name
    )['StreamDescriptionSummary']['StreamStatus']
    if status == 'ACTIVE':
        break
    time.sleep(1)
else:
    raise RuntimeError(f'Stream {stream_name!r} did not become ACTIVE: {status}')

records = [
    {"Data": "Hi everyone", "PartitionKey": "joao"},
    {"Data": "How are you?", "PartitionKey": "maria"},
    {"Data": "Everything okay?", "PartitionKey": "joao"},
]

for index, record in enumerate(records):
    kinesis.put_record(
        StreamName=stream_name,
        Data=record["Data"].encode("utf-8"),
        PartitionKey=record["PartitionKey"],
    )

    # Each record gets a different ApproximateArrivalTimestamp.
    if index < len(records) - 1:
        time.sleep(1)

print("Stream created successfully.")
