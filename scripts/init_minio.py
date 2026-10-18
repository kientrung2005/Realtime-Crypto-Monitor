import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.settings import settings
from src.storage.minio_client import MinIOStorageClient


logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    storage = MinIOStorageClient()
    deadline = time.monotonic() + 120

    while True:
        try:
            storage.initialize()
            logger.info(
                "MinIO bucket %s is ready with %d-day retention",
                settings.MINIO_BUCKET,
                settings.MINIO_RETENTION_DAYS,
            )
            return
        except Exception:
            if time.monotonic() >= deadline:
                raise
            logger.info("Waiting for MinIO to become ready")
            time.sleep(2)


if __name__ == "__main__":
    main()
