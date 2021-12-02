"""
Methods that access S3
"""
import boto3
import logging
from io import StringIO, BytesIO
import pandas as pd

from xetra.common.constants import S3FileTypes
from xetra.common.common_exceptions import WrongFormatException


class S3BucketConnector:
    """
    Class for S3 interactions.
    """

    def __init__(self, end_point_url: str, bucket: str, profile_name: str):
        """
        :param end_point_url: end point url to s3 bucket
        :param bucket: s3 bucket name we will use
        :param profile_name: aws profile in order to access S3 bucket
        """
        self._logger = logging.getLogger(__name__)
        self.end_point_url = end_point_url
        self.session = boto3.session.Session(profile_name=profile_name)
        self._s3 = self.session.resource(service_name='s3', endpoint_url=end_point_url)
        self._bucket = self._s3.Bucket(bucket)

    def list_file_in_prefix(self, prefix: str):
        """
        list all files with prefix on S3 bucket
        :param prefix: prefix that s3 file names will be filtered with
        :return: list of all file name contaning the prefix in key
        """
        return [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]

    def read_csv_to_data_frame(self, key: str, encoding='utf-8', separator=','):
        """
        Read csv file from S3 and return data frame

        :param key:key of the file that will be read
        :param encoding: encoding of data inside the csv file
        :param separator: separator of CSV file
        :return:
        """
        self._logger.info('Reading file %s/%s/%s', self.end_point_url, self._bucket.name, key)
        csv_obj = self._bucket.Object(key=key).get()['Body'].read().decode(encoding)
        data = StringIO(csv_obj)
        data_frame = pd.read_csv(data, sep=separator)
        return data_frame

    def write_df_to_s3_bucket(self, data_frame: pd.DataFrame, key: str, file_format: str):
        """
        Writing a Pandas data frame to S3
        supports the following formats: .csv, .parquet

        :param data_frame: Pandas data frame that should be written
        :param key: target key of the saved file
        :param file_format: format of the saved file
        :return:
        """
        if data_frame.empty:
            self._logger.info('The data frame is empty! No file will be written')
            return None

        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)

        elif file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, engine='pyarrow', index=False)
            return self.__put_object(out_buffer, key)

        self._logger.info('The file format %s isnt supported to be written to S3', file_format)
        raise WrongFormatException

    def __put_object(self, out_buffer: StringIO or BytesIO, key: str):
        """
        Helper function for self.write_df_to_s3()
        :param out_buffer: StringIO | BytesIO
        :param key: target key of the saved file
        """
        self._logger.info('Writing file to %s/%s/%s', self.end_point_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True
