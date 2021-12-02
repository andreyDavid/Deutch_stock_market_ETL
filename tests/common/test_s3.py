""" Test S3 bucket connector method"""

import unittest
import boto3
import pandas as pd
from moto import mock_s3
from io import StringIO, BytesIO

from xetra.common.s3 import S3BucketConnector
from xetra.common.common_exceptions import WrongFormatException


class TestS3BucketConnector(unittest.TestCase):
    """
    Testing the S3BucketConnector class.
    """

    def setUp(self):
        """
        setting up the environment
        """
        #mock s3 connection
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        # Definig class arguments
        self.s3_endpoint_url = 'https://s3.eu-central1-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'
        self.profile_name = 'UnitTest'

        # access aws using boto 3 and a profile name deticated for testing
        session = boto3.session.Session(profile_name='UnitTest')

        # create a bucket on s3
        self.s3 = session.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration= {
                                  'LocationConstraint': 'eu-central-1'
                              })
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)

        # creating a bucket on mocked s3
        self.s3_bucket_conn = S3BucketConnector(end_point_url=self.s3_endpoint_url,
                                                bucket=self.s3_bucket_name,
                                                profile_name= self.profile_name)

    def tearDown(self):
        """
        Execute after unittest is done
        """
        # stopping mock s3 connection
        self.mock_s3.stop()

    def test_list_files_in_prefix_ok(self):
        """
        Tests the list_files_in_prefix method for getting 2 file keys
        as list on the mocked s3 bucket
        """
        # expected result
        prefix_exp = 'prefix/'
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'

        # test inited
        csv_content = """col1,col2
        valA,ValB"""
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)

        #method execution
        list_result = self.s3_bucket_conn.list_file_in_prefix(prefix_exp)

        # Tests after method execution
        self.assertEqual(len(list_result),2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)

        #Clean up - delete s3 content
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key1_exp
                    },
                    {
                        'Key': key1_exp
                    }
                ]
            }
        )

    def test_list_files_in_prefix_wrong_prefix(self):
        """
        Tests the list_files_in_prefix method for wrong or non existing prefix
        """
        # expected result
        prefix_exp = 'prefix/'

        # method execution
        list_result = self.s3_bucket_conn.list_file_in_prefix(prefix_exp)

        # Tests after method execution
        self.assertTrue(not list_result)

    def test_read_csv_to_df_success(self):
        """
        test the read_csv_to_df method
        reads 1 .csv file from the mocked s3 bucked
        """
        # Expected result
        key_exp = 'test.csv'
        col1_exp = 'col1'
        col2_exp = 'col2'
        val1_exp = 'val1'
        val2_exp = 'val2'

        #Test init
        csv_content = f'{col1_exp}{col2_exp}\n{val1_exp}{val2_exp}'
        self.s3_bucket.put_object(Body=csv_content, Key=key_exp)

        # Method execution
        with self.assertLogs() as logm:
            df_result = self.s3_bucket_conn.read_csv_to_data_frame(key_exp)

        #test after method execution
        self.assertEqual(df_result.shape[0],1)
        self.assertEqual(df_result.shape[1],2)
        self.assertEqual(val1_exp, df_result[col1_exp][0])
        self.assertEqual(val2_exp, df_result[col2_exp][0])

        #cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )

    def test_write_df_to_s3_empty(self):
        """
        Tests write_df_to_s3 method with an empty data frame
        """
        # expected result
        return_exp = None

        # Test init
        df_empty = pd.DataFrame()
        key = 'key.csv'
        file_format = 'csv'

        # method execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3_bucket(df_empty, key, file_format)

        # test after method executes
        self.assertEqual(return_exp, result)

    def test_write_df_from_type_csv_to_s3(self):
        """
        Tests write_df_to_s3 method with an csv data frame
        """
        # expected result
        return_exp = True
        df_exp = pd.DataFrame([['A','B'], ['C', 'D']], columns=['col1', 'col2'])
        key_exp = 'test.csv'

        # Test init
        file_format = 'csv'

        # Method execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3_bucket(df_exp, key_exp, file_format)

        # test after method execution
        data = self.s3_bucket.Object(key=key_exp).get()['Body'].read().decode('utf-8')
        out_buffer = StringIO(data)
        df_result = pd.read_csv(out_buffer)
        self.assertEqual(return_exp, result)
        self.assertTrue(df_exp.equals(df_result))

        # cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )

    def test_write_df_from_type_prquet_to_s3(self):
        """
        Tests write_df_to_s3 method with an parquet data frame
        """
        # expected result
        return_exp = True
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']], columns=['col1', 'col2'])
        key_exp = 'test.parquet'

        # Test init
        file_format = 'parquet'

        # Method execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3_bucket(df_exp, key_exp, file_format)

        # test after method execution
        data = self.s3_bucket.Object(key=key_exp).get()['Body'].read().decode('utf-8')
        out_buffer = StringIO(data)
        df_result = pd.read_csv(out_buffer)
        self.assertEqual(return_exp, result)
        self.assertTrue(df_exp.equals(df_result))

        # cleanup after test
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )

    def test_write_df_with_wrong_format_to_s3(self):
        """
        Tests write_df_to_s3 method with a wrong format data frame
        """

        # Expected results
        df_exp = pd.DataFrame([['A', 'B'], ['C', 'D']], columns=['col1', 'col2'])
        key_exp = 'test.parquet'
        format_exp = 'wrong_format'
        exception_exp = WrongFormatException

        # Method execution
        with self.assertLogs() as logm:
            with self.assertRaises(exception_exp):
                self.s3_bucket_conn.write_df_to_s3_bucket(df_exp, key_exp, format_exp)


if __name__ == "__main__":
    unittest.main()
