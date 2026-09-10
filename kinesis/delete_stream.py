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


def delete_stream():
    try:
        kinesis.delete_stream(StreamName=STREAM_NAME)
    except kinesis.exceptions.ResourceNotFoundException:
        print(f'Stream {STREAM_NAME!r} was not found.')
        return

    print(f'Stream {STREAM_NAME!r} deleted successfully.')


if __name__ == '__main__':
    delete_stream()
