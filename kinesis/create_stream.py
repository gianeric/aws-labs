import boto3
import time

# LocalStack roda como serviço Docker, não precisa importar
LOCALSTACK_ENDPOINT = 'http://localhost:4566'

kinesis = boto3.client(
    'kinesis',
    endpoint_url=LOCALSTACK_ENDPOINT,  # Use a string diretamente
    region_name='sa-east-1',
    aws_access_key_id='1234',
    aws_secret_access_key='1234'
)

# Criar stream com throughput limitado, caso ainda não exista.
stream_name = 'chat-messages'
try:
    kinesis.create_stream(
        StreamName=stream_name,
        ShardCount=2
    )
except kinesis.exceptions.ResourceInUseException:
    pass

# A criação é assíncrona; só é permitido gravar quando o stream estiver ACTIVE.
for _ in range(30):
    status = kinesis.describe_stream_summary(
        StreamName=stream_name
    )['StreamDescriptionSummary']['StreamStatus']
    if status == 'ACTIVE':
        break
    time.sleep(1)
else:
    raise RuntimeError(f'Stream {stream_name!r} não ficou ACTIVE: {status}')

records = [
    {"Data": "Oi pessoal", "PartitionKey": "joao"},
    {"Data": "Como vai?", "PartitionKey": "maria"},
    {"Data": "Tudo bem?", "PartitionKey": "joao"},
]

for index, record in enumerate(records):
    kinesis.put_record(
        StreamName=stream_name,
        Data=record["Data"].encode("utf-8"),
        PartitionKey=record["PartitionKey"],
    )

    # Cada registro recebe um ApproximateArrivalTimestamp diferente.
    if index < len(records) - 1:
        time.sleep(1)

print("Stream criada com sucesso.")
