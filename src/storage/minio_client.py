from minio import Minio
from minio.commonconfig import ENABLED, Filter
from minio.lifecycleconfig import Expiration, LifecycleConfig, Rule

from configs.settings import settings


class MinIOStorageClient:
    def __init__(self) -> None:
        self.client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
            region=settings.MINIO_REGION,
        )

    def ensure_bucket_exists(self) -> None:
        if self.client.bucket_exists(bucket_name=settings.MINIO_BUCKET):
            return

        self.client.make_bucket(
            bucket_name=settings.MINIO_BUCKET,
            location=settings.MINIO_REGION,
        )

    def configure_lifecycle(self) -> None:
        lifecycle = LifecycleConfig(
            [
                Rule(
                    status=ENABLED,
                    rule_filter=Filter(prefix=""),
                    rule_id="archive-retention",
                    expiration=Expiration(
                        days=settings.MINIO_RETENTION_DAYS,
                    ),
                )
            ]
        )
        self.client.set_bucket_lifecycle(
            bucket_name=settings.MINIO_BUCKET,
            config=lifecycle,
        )

    def initialize(self) -> None:
        self.ensure_bucket_exists()
        self.configure_lifecycle()
