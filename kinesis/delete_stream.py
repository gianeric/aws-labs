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

def deletar_stream():
    try:
        kinesis.delete_stream(StreamName=STREAM_NAME)
    except kinesis.exceptions.ResourceNotFoundException:
        print(f'A stream {STREAM_NAME!r} nao foi encontrada.')
        return

    print(f'Stream {STREAM_NAME!r} excluida com sucesso.')


if __name__ == '__main__':
    deletar_stream()
