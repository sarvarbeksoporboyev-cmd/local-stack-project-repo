"""Create LocalStack resources for the Helsinki bike pipeline."""

from __future__ import annotations

import argparse
import json
import sys
import time
import zipfile
from pathlib import Path

from botocore.exceptions import ClientError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from helsinki_pipeline.aws.aws_clients import create_client  # noqa: E402
from helsinki_pipeline.config import AppConfig  # noqa: E402


class LocalStackBootstrapper:
    """Provision required AWS resources in LocalStack for the assignment."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.s3 = create_client("s3", config)
        self.sns = create_client("sns", config)
        self.sqs = create_client("sqs", config)
        self.lambda_client = create_client("lambda", config)
        self.dynamodb = create_client("dynamodb", config)

    def run(self) -> dict[str, str]:
        """Provision bucket, pub/sub resources, DynamoDB tables, and Lambda."""

        self.ensure_bucket()
        topic_arn, queue_url, queue_arn = self.ensure_topic_and_queue()
        self.ensure_s3_notification(topic_arn)
        self.ensure_dynamodb_tables()
        lambda_arn = self.ensure_lambda(queue_arn)

        outputs = {
            "bucket": self.config.s3_bucket_name,
            "topic_arn": topic_arn,
            "queue_url": queue_url,
            "queue_arn": queue_arn,
            "lambda_arn": lambda_arn,
        }
        self.write_outputs(outputs)
        return outputs

    def ensure_bucket(self) -> None:
        """Create S3 bucket if not present."""

        try:
            self.s3.head_bucket(Bucket=self.config.s3_bucket_name)
            return
        except ClientError:
            pass

        if self.config.aws_region == "us-east-1":
            self.s3.create_bucket(Bucket=self.config.s3_bucket_name)
        else:
            self.s3.create_bucket(
                Bucket=self.config.s3_bucket_name,
                CreateBucketConfiguration={"LocationConstraint": self.config.aws_region},
            )

    def ensure_topic_and_queue(self) -> tuple[str, str, str]:
        """Create SNS topic + SQS queue and subscribe queue to topic."""

        topic_arn = self.sns.create_topic(Name=self.config.sns_topic_name)["TopicArn"]

        queue_url = self.sqs.create_queue(QueueName=self.config.sqs_queue_name)["QueueUrl"]
        queue_attrs = self.sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["QueueArn"],
        )["Attributes"]
        queue_arn = queue_attrs["QueueArn"]

        queue_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowSnsToSend",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "sqs:SendMessage",
                    "Resource": queue_arn,
                    "Condition": {
                        "ArnEquals": {
                            "aws:SourceArn": topic_arn,
                        }
                    },
                }
            ],
        }
        self.sqs.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={"Policy": json.dumps(queue_policy)},
        )

        subscriptions = self.sns.list_subscriptions_by_topic(TopicArn=topic_arn).get(
            "Subscriptions",
            [],
        )
        if not any(sub.get("Endpoint") == queue_arn for sub in subscriptions):
            self.sns.subscribe(
                TopicArn=topic_arn,
                Protocol="sqs",
                Endpoint=queue_arn,
            )

        topic_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowS3Publish",
                    "Effect": "Allow",
                    "Principal": {"Service": "s3.amazonaws.com"},
                    "Action": "sns:Publish",
                    "Resource": topic_arn,
                    "Condition": {
                        "ArnLike": {
                            "aws:SourceArn": f"arn:aws:s3:::{self.config.s3_bucket_name}",
                        }
                    },
                }
            ],
        }
        self.sns.set_topic_attributes(
            TopicArn=topic_arn,
            AttributeName="Policy",
            AttributeValue=json.dumps(topic_policy),
        )

        return topic_arn, queue_url, queue_arn

    def ensure_s3_notification(self, topic_arn: str) -> None:
        """Configure S3 object-created notifications to SNS topic."""

        self.s3.put_bucket_notification_configuration(
            Bucket=self.config.s3_bucket_name,
            NotificationConfiguration={
                "TopicConfigurations": [
                    {
                        "Id": "csv-created-event",
                        "TopicArn": topic_arn,
                        "Events": ["s3:ObjectCreated:*"],
                        "Filter": {
                            "Key": {
                                "FilterRules": [
                                    {"Name": "suffix", "Value": ".csv"},
                                ]
                            }
                        },
                    }
                ]
            },
        )

    def ensure_dynamodb_tables(self) -> None:
        """Create DynamoDB tables for raw rows, period metrics, and file state."""

        self._ensure_table(
            table_name=self.config.dynamodb_raw_table,
            key_schema=[{"AttributeName": "trip_id", "KeyType": "HASH"}],
            attr_defs=[{"AttributeName": "trip_id", "AttributeType": "S"}],
        )

        self._ensure_table(
            table_name=self.config.dynamodb_period_metrics_table,
            key_schema=[
                {"AttributeName": "period_type", "KeyType": "HASH"},
                {"AttributeName": "period_value", "KeyType": "RANGE"},
            ],
            attr_defs=[
                {"AttributeName": "period_type", "AttributeType": "S"},
                {"AttributeName": "period_value", "AttributeType": "S"},
            ],
        )

        self._ensure_table(
            table_name=self.config.dynamodb_file_state_table,
            key_schema=[{"AttributeName": "file_id", "KeyType": "HASH"}],
            attr_defs=[{"AttributeName": "file_id", "AttributeType": "S"}],
        )

    def _ensure_table(
        self,
        table_name: str,
        key_schema: list[dict[str, str]],
        attr_defs: list[dict[str, str]],
    ) -> None:
        """Create one DynamoDB table if it does not already exist."""

        try:
            self.dynamodb.describe_table(TableName=table_name)
            return
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code != "ResourceNotFoundException":
                raise

        self.dynamodb.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attr_defs,
            BillingMode="PAY_PER_REQUEST",
        )

        for _ in range(30):
            status = self.dynamodb.describe_table(TableName=table_name)["Table"]["TableStatus"]
            if status == "ACTIVE":
                return
            time.sleep(1)

    def ensure_lambda(self, queue_arn: str) -> str:
        """Create or update Lambda function and SQS event source mapping."""

        lambda_zip_path = self._build_lambda_zip()
        zip_bytes = lambda_zip_path.read_bytes()

        env_vars = {
            "AWS_ACCESS_KEY_ID": self.config.aws_access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.config.aws_secret_access_key,
            "AWS_REGION": self.config.aws_region,
            "LOCALSTACK_ENDPOINT": self.config.localstack_endpoint,
            "S3_BUCKET_NAME": self.config.s3_bucket_name,
            "RAW_PREFIX": self.config.raw_prefix,
            "METRICS_PREFIX": self.config.metrics_prefix,
            "DYNAMODB_RAW_TABLE": self.config.dynamodb_raw_table,
            "DYNAMODB_PERIOD_METRICS_TABLE": self.config.dynamodb_period_metrics_table,
            "DYNAMODB_FILE_STATE_TABLE": self.config.dynamodb_file_state_table,
        }

        try:
            existing = self.lambda_client.get_function(
                FunctionName=self.config.lambda_function_name,
            )
            lambda_arn = existing["Configuration"]["FunctionArn"]
            self.lambda_client.update_function_code(
                FunctionName=self.config.lambda_function_name,
                ZipFile=zip_bytes,
            )
            self.lambda_client.update_function_configuration(
                FunctionName=self.config.lambda_function_name,
                Runtime="python3.10",
                Handler="lambda_function.lambda_handler",
                Timeout=900,
                MemorySize=1024,
                Environment={"Variables": env_vars},
            )
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code != "ResourceNotFoundException":
                raise

            create_resp = self.lambda_client.create_function(
                FunctionName=self.config.lambda_function_name,
                Runtime="python3.10",
                Handler="lambda_function.lambda_handler",
                Role=self.config.lambda_role_arn,
                Timeout=900,
                MemorySize=1024,
                Code={"ZipFile": zip_bytes},
                Environment={"Variables": env_vars},
                Publish=True,
            )
            lambda_arn = create_resp["FunctionArn"]

        mappings = self.lambda_client.list_event_source_mappings(
            FunctionName=self.config.lambda_function_name,
            EventSourceArn=queue_arn,
        ).get("EventSourceMappings", [])

        if not mappings:
            self.lambda_client.create_event_source_mapping(
                FunctionName=self.config.lambda_function_name,
                EventSourceArn=queue_arn,
                BatchSize=10,
                Enabled=True,
            )

        return lambda_arn

    def _build_lambda_zip(self) -> Path:
        """Build a lightweight Lambda package if a prebuilt package does not exist."""

        prebuilt = PROJECT_ROOT / "build" / "dynamodb_ingestor.zip"
        if prebuilt.exists():
            return prebuilt

        source_file = PROJECT_ROOT / "lambdas" / "dynamodb_ingestor" / "lambda_function.py"
        build_dir = PROJECT_ROOT / "build"
        build_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(prebuilt, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.write(source_file, "lambda_function.py")

        return prebuilt

    @staticmethod
    def write_outputs(outputs: dict[str, str]) -> None:
        """Persist bootstrap output to a JSON file for later scripts."""

        out_path = PROJECT_ROOT / "config" / "runtime_resources.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(outputs, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--docker-endpoint",
        action="store_true",
        help="Use docker network endpoint (localstack:4566).",
    )
    return parser.parse_args()


def main() -> None:
    """Execute full LocalStack bootstrap flow."""

    args = parse_args()
    config = AppConfig.from_env(PROJECT_ROOT / ".env", use_docker_endpoint=args.docker_endpoint)
    bootstrapper = LocalStackBootstrapper(config)
    outputs = bootstrapper.run()

    print("LocalStack resources provisioned:")
    for key, value in outputs.items():
        print(f"- {key}: {value}")


if __name__ == "__main__":
    main()

