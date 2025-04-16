#!/bin/bash
# mock/localstack_setup.sh - Script to launch localstack S3 mock for testing

set -e

SERVICES="s3"
REGION="us-east-1"
BUCKET_NAME="mock-annex-bucket"

# Start LocalStack
if ! docker ps | grep -q localstack; then
    echo "Starting LocalStack..."
    docker run --rm -d --name localstack \
           -e SERVICES=$SERVICES \
           -e DEFAULT_REGION=$REGION \
           -p 4566:4566 \
           localstack/localstack
    sleep 5
fi

# Create S3 bucket
aws --endpoint-url=http://localhost:4566 s3 mb s3://$BUCKET_NAME || true

echo "LocalStack is ready. Bucket $BUCKET_NAME created."
echo "You can test using AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test and endpoint-url=http://localhost:4566"

