import os

BUGOUT_DRONES_TOKEN = os.environ.get("BUGOUT_DRONES_TOKEN")
if BUGOUT_DRONES_TOKEN is None or BUGOUT_DRONES_TOKEN == "":
    raise ValueError("BUGOUT_DRONES_TOKEN environment variable must be set")
BUGOUT_DRONES_TOKEN_HEADER = os.environ.get("BUGOUT_DRONES_TOKEN_HEADER")
if BUGOUT_DRONES_TOKEN_HEADER is None or BUGOUT_DRONES_TOKEN_HEADER == "":
    raise ValueError("BUGOUT_DRONES_TOKEN_HEADER environment variable must be set")

# S3
DRONES_BUCKET = os.environ.get("AWS_S3_DRONES_BUCKET")
if DRONES_BUCKET is None:
    raise ValueError("AWS_S3_DRONES_BUCKET environment variable must be set")
DRONES_BUCKET_STATISTICS_PREFIX_RAW = os.environ.get(
    "AWS_S3_DRONES_BUCKET_STATISTICS_PREFIX"
)
if DRONES_BUCKET_STATISTICS_PREFIX_RAW is None:
    raise ValueError("DRONES_BUCKET_STATISTICS_PREFIX environment variable must be set")
DRONES_BUCKET_STATISTICS_PREFIX = DRONES_BUCKET_STATISTICS_PREFIX_RAW.rstrip("/")

STATISTICS_S3_PRESIGNED_URL_EXPIRATION_TIME = 60  # seconds

BUGOUT_REDIS_URL = os.getenv("BUGOUT_REDIS_URL")
BUGOUT_REDIS_PASSWORD = os.getenv("BUGOUT_REDIS_PASSWORD")
REPORTS_CHUNK_SIZE = int(os.getenv("REPORTS_CHUNK_SIZE", "100"))
WATING_UNTIL_NEW_REPORTS = int(os.getenv("WAITING_UNTIL_NEW_REPORTS", "10"))
