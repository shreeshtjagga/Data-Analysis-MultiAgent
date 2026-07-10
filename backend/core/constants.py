import os

APP_VERSION = '2.0.0'
PIPELINE_VERSION = 'v8'

# ── Data persistence ────────────────────────────────────────────────────────
# Maximum rows stored in the PostgreSQL clean_data column.
# Used as the reliable DB-backed fallback when Parquet disk cache is unavailable.
MAX_CLEAN_STORE_ROWS: int = int(os.getenv('MAX_CLEAN_STORE_ROWS', '5000'))

# Whether to write/read Parquet files on the local disk.
# Set ENABLE_DISK_CACHE=false on stateless platforms (Render, Railway, Fly.io)
# where the filesystem is ephemeral. Set to true (or omit) for local dev or
# deployments with a persistent volume mounted at PARQUET_STORAGE_DIR.
ENABLE_DISK_CACHE: bool = os.getenv('ENABLE_DISK_CACHE', 'true').lower() == 'true'

# Absolute path to the Parquet storage directory.
# Override with PARQUET_STORAGE_DIR env var to point at a mounted volume in prod.
_DEFAULT_STORAGE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'storage', 'data')
PARQUET_STORAGE_DIR: str = os.path.normpath(os.getenv('PARQUET_STORAGE_DIR', _DEFAULT_STORAGE_DIR))
