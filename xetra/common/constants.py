""" stores project constants"""

from enum import Enum


class S3FileTypes(Enum):
    """
    SUpported file types for S3 bucket connector
    """
    CSV = 'csv'
    PARQUET = 'parquet'


class MetaProcessFormat(Enum):
    """
    commonly used formats for meta process class
    """
    META_DATE_FORMAT = '%Y-%m-%d'
    META_PROCESS_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    META_SOURCE_DATE_COL = 'source_date'
    META_PROCESS_COL = 'datetime_of_processing'
    META_FILE_FORMAT = 'csv'
