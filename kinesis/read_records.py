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

def ler_shard(shard_id):
    resposta = kinesis.get_shard_iterator(
        StreamName=STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType='TRIM_HORIZON'
    )

    registros = kinesis.get_records(
        ShardIterator=resposta['ShardIterator']
    )['Records']

    return registros

def listar_shards():
    resposta = kinesis.list_shards(StreamName=STREAM_NAME)
    return resposta.get('Shards', [])

def ler_stream():
    print(f'\nStream: {STREAM_NAME}')
    print('Buscando shards...')

    try:
        shards = listar_shards()
    except kinesis.exceptions.ResourceNotFoundException:
        raise SystemExit(f'A stream {STREAM_NAME!r} nao foi encontrada.')

    if not shards:
        raise SystemExit('A stream existe, mas ainda nao possui shards.')

    print(f'{len(shards)} shard(s) encontrado(s).\n')

    registros_da_stream = []
    for shard in shards:
        shard_id = shard['ShardId'] #ex: shardId-000000000001 | shardId-000000000002
        registros = ler_shard(shard_id)

        for registro in registros:
            registros_da_stream.append({
                'shard_id': shard_id,
                'registro': registro,
            })

    registros_da_stream.sort(
        key=lambda item: item['registro']['ApproximateArrivalTimestamp']
    )

    if not registros_da_stream:
        print('Nenhum registro encontrado.')
    else:
        print('Registros ordenados por horario de chegada:\n')
        for item in registros_da_stream:
            registro = item['registro']
            mensagem = registro['Data'].decode('utf-8')
            chegada = registro['ApproximateArrivalTimestamp']
            print(f'[{item["shard_id"]}] {chegada.isoformat()}')
            print(f'  Usuario: {registro["PartitionKey"]}')
            print(f'  Mensagem: {mensagem}\n')

    print(
        f'Leitura concluida: {len(registros_da_stream)} '
        'registro(s) encontrado(s).'
    )


if __name__ == '__main__':
    ler_stream()
