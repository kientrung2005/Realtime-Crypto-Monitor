import os
from urllib.parse import quote_plus


SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

metadata_user = quote_plus(os.getenv("SUPERSET_METADATA_USER", "superset"))
metadata_password = quote_plus(
    os.getenv("SUPERSET_METADATA_PASSWORD", "superset")
)
metadata_host = os.getenv("SUPERSET_METADATA_HOST", "superset-db")
metadata_port = os.getenv("SUPERSET_METADATA_PORT", "5432")
metadata_database = quote_plus(
    os.getenv("SUPERSET_METADATA_DATABASE", "superset")
)

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{metadata_user}:{metadata_password}"
    f"@{metadata_host}:{metadata_port}/{metadata_database}"
)

WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False
ENABLE_PROXY_FIX = True
SQLALCHEMY_TRACK_MODIFICATIONS = False
