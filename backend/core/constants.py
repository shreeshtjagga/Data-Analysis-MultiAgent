import os

APP_VERSION = '2.0.0'
PIPELINE_VERSION = 'v7'

MAX_CLEAN_STORE_ROWS: int = int(os.getenv('MAX_CLEAN_STORE_ROWS', '5000'))

ENABLE_DISK_CACHE: bool = os.getenv('ENABLE_DISK_CACHE', 'true').lower() == 'true'

_DEFAULT_STORAGE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'storage', 'data')
PARQUET_STORAGE_DIR: str = os.path.normpath(os.getenv('PARQUET_STORAGE_DIR', _DEFAULT_STORAGE_DIR))