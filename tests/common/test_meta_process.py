import unittest

import boto3
import pandas as pd
from moto import mock_s3
from datetime import datetime, timedelta
from io import StringIO

from xetra.common.constants import MetaProcessFormat
from xetra.common.meta_process import MetaProcess
from xetra.common.s3 import S3BucketConnector


class TestMetaProcessMethods(unittest.TestCase):
    """
    Testing MetaProcess class.
    """

    def setUp(self):
        """
        setting up the environment
        """
        # Mock s3 connection
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        # Defining class arguments
        self.s3_endpoint_url = 'https://s3.eu-central1-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'
        self.profile_name = 'UnitTest'

        # Access aws using boto 3 and a profile name deticated for testing
        session = boto3.session.Session(profile_name='UnitTest')

        # Create a bucket on s3
        self.s3 = session.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration= {
                                  'LocationConstraint': 'eu-central-1'
                              })
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)

        # Creating a bucket on mocked s3
        self.s3_bucket_meta = S3BucketConnector(end_point_url=self.s3_endpoint_url,
                                                bucket=self.s3_bucket_name,
                                                profile_name=self.profile_name)

    def tearDown(self):
        """
        Execute after unittest is done
        """
        # stopping mock s3 connection
        self.mock_s3.stop()

    def test_update_meta_file_no_meta_file(self):
        """
        Tests the update_meta_file method when there is no meta file
        """

        # Expected result
        date_list_exp = ['2021-04-16', '2021-04-17']
        proc_date_list_exp = [datetime.today().date()] * 2

        # Test init
        meta_key = 'meta.csv'

        # Method execution
        MetaProcess.update_meta_file(date_list_exp, meta_key, self.s3_bucket_meta)

        # Read meta file
        data = self.s3_bucket.Object(key=meta_key).get()['Body'].read().decode('utf-8')
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result = list(df_meta_result[MetaProcessFormat.META_SOURCE_DATE_COL.value])
        proc_date_list_result = list(
            pd.to_datetime(df_meta_result[MetaProcessFormat.META_PROCESS_COL.value]).datetime.date
        )

        # Test after method execution
        self.assertEqual(date_list_exp, date_list_result)
        self.assertEqual(proc_date_list_exp, proc_date_list_result)

        # Clean up - delete s3 content
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_update_meta_file_empty_date_list(self):
        """
        Tests the update_meta_file method
        when the argument extract_date_list is empty
        """

        # Expected result
        return_exp = True

        # Test init
        meta_key = 'meta.csv'
        date_list = []

        # Method execution
        result = MetaProcess.update_meta_file(date_list, meta_key, self.s3_bucket_meta)

        # Test after method execution
        self.assertIn(return_exp, result)

    def test_update_meta_file_is_successful(self):
        """
        Tests the update_meta_file method
        when the argument extract_date_list is empty
        """

        # Expected result
        date_list_old = ['2021-04-12', '2021-04-13']
        date_list_new = ['2021-04-16', '2021-04-17']
        date_list_exp = date_list_old + date_list_new
        proc_date_list_exp = [datetime.today().date()] * 4

        # Test init
        meta_key = 'meta.csv'
        meta_content = (
            f'{MetaProcessFormat.META_SOURCE_DATE_COL.val},'
            f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{date_list_old[0]},'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESSDATE_FORMAT.value)}\n'
            f'{date_list_old[1]}'
            f'{datetime.today().strftime(MetaProcessFormat.META_PROCESSDATE_FORMAT.value)}'
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        # Method execution
        result = MetaProcess.update_meta_file(date_list_new, meta_key, self.s3_bucket_meta)

        # Read meta file
        data = self.s3_bucket.Object(key=meta_key).get()['Body'].read().decode('utf-8')
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result = list(df_meta_result[MetaProcessFormat.META_SOURCE_DATE_COL.value])
        proc_date_list_result = list(df_meta_result[MetaProcessFormat.META_PROCESS_COL.value]).dt.date

        # Clean up - delete s3 content
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_update_meta_file_with_wrong_meta_file_data(self):
        """
        Tests the update_meta_file method
        whine there is a wrong meta file
        """

        # Expected result
        date_list_old = ['2021-04-12', '2021-04-13']
        date_list_new = ['2021-04-16', '2021-04-17']

        # Test init
        meta_key = 'meta.csv'
        meta_content = (
            f'wrong_column, {MetaProcessFormat.META_SOURCE_DATE_COL.val},'
            f'{MetaProcessFormat.META_PROCESS_COL.value}\n'
            f'{date_list_old[ 0 ]},'
            f'{datetime.today().strftime( MetaProcessFormat.META_PROCESSDATE_FORMAT.value )}\n'
            f'{date_list_old[ 1 ]}'
            f'{datetime.today().strftime( MetaProcessFormat.META_PROCESSDATE_FORMAT.value )}'
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)

        # Method execution
        with self.assertRaises(Body=meta_content, Key=meta_key):
            MetaProcess.update_meta_file(date_list_new, meta_key, self.s3_bucket_meta)

        # Clean up - delete s3 content
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': meta_key
                    }
                ]
            }
        )

    def test_return_date_list_no_meta_file(self):
        """
        Tests the return_date_list method
        when there is no meta file
        """

        # Expected result
        date_list_exp = [(datetime.today().date() - timedelta(days=day)).strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value) for day in range(4)]
        min_date_exp = (datetime.today().date() - timedelta(days=2)).strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)

        # Test init
        meta_key = 'meta.csv'
        first_date = min_date_exp

        # Method execution
        min_date_return, date_list_return = MetaProcess.return_date_list(first_date, meta_key, self.s3_bucket_meta)

        # Test after method execution
        self.assertEqual(set(date_list_exp), set(date_list_return))
        self.assertEqual(min_date_exp, min_date_return)


if __name__ == "__main__":
    unittest.main()
