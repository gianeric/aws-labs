# AWS Labs

Hands-on labs for studying and experimenting with AWS services using Python, Boto3, and LocalStack.

## Services

- Kinesis Data Streams
- SQS
- SNS
- DynamoDB
- Lambda
- More to come

## LocalStack with Docker Compose

To start the LocalStack container for this project:

1. Open a terminal in the project root.
2. Run:

```bash
docker compose -f docker/docker-compose.yml up -d
```

3. Confirm that the container is running:

```bash
docker ps
```

4. Check the LocalStack endpoint:

```bash
curl http://localhost:4566/_localstack/health
```

If the endpoint is healthy, you can use the Kinesis scripts from the `kinesis` folder.

## Kinesis Data Streams

A simple hands-on lab covering:

- Stream creation
- Shard configuration
- Record publishing
- Record reading
- Partition keys
- Stream deletion

**Technologies:** Python, Boto3, LocalStack

## How to test the Kinesis flow

From the project root, run:

```bash
python kinesis/create_stream.py
python kinesis/read_records.py
```

To delete the test stream:

```bash
python kinesis/delete_stream.py
```

> The LocalStack data is stored in the `volume` directory, which is ignored by Git.