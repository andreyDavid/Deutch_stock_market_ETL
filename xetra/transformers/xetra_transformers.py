from typing import NamedTuple
import logging
import pandas as pd
from datetime import datetime
from xetra.common.meta_process import MetaProcess
from xetra.common.s3 import S3BucketConnector


class XetraSourceConfig(NamedTuple):
    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str


class XetraTargetConfig(NamedTuple):

    trg_col_isin: str
    trg_col_date: str
    trg_col_op_price: str
    trg_col_clos_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_dail_trad_vol: str
    trg_col_ch_prev_clos: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str


class XetraETL:

    def __init__(self,
                 s3_bucket_source: S3BucketConnector,
                 s3_bucket_target: S3BucketConnector,
                 meta_key: str,
                 src_args: XetraSourceConfig,
                 trg_args: XetraTargetConfig):

        self._logger = logging.getLogger(__name__)
        self.s3_bucket_source = s3_bucket_source
        self.s3_bucket_trg = s3_bucket_target
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args
        self.extract_date, self.extract_date_list = MetaProcess.return_date_list(self.src_args.src_first_extract_date,
                                                                                 self.meta_key, self.s3_bucket_trg)
        self.meta_update_list = [date for date in self.extract_date_list if date >= self.extract_date]

    def extract(self):
        """
        Read the source data and concatenates them to one Pandas DataFrame.
        :return:
            data_frame: Pandas DataFrame with the extracted data
        """
        self._logger.info('Extracting Xetra source files started...')
        files = [key for date in self.extract_date_list
                     for key in self.s3_bucket_source.list_file_in_prefix(date)]
        if not files:
            data_frame = pd.DataFrame()
        else:
            data_frame = pd.concat([self.s3_bucket_source.read_csv_to_data_frame(file) for file in files], ignore_index=True)
        self._logger.info('Extraction Xetra source files finished.')

        return data_frame

    def transform_report1(self, data_frame: pd.DataFrame):
        """
        Applies the necessary transformation to create report 1

        :param data_frame: Pandas Data frame

        :return:
            data_frame: Transformed Pandas DataFrame as Output
        """

        if data_frame.empty:
            self._logger.info('The dataframe is empty. No transformations will be applied.')
            return data_frame
        self._logger.info('Applying transformations to Xetra source data for report 1 started...')

        # Filtering necessary source columns
        data_frame = data_frame.loc[:, self.src_args.src_columns]

        # Removing rows with missing values
        data_frame.dropna(inplace=True)

        # Calculating opening price per ISIN and day
        data_frame[self.trg_args.trg_col_op_price] = data_frame\
            .sort_values(by=[self.src_args.src_col_time])\
            .groupby([
                    self.src_args.src_col_isin,
                    self.src_args.src_col_date
                    ])[self.src_args.src_col_start_price]\
            .transform('first')

        # Calculating closing price per ISIN and day
        data_frame[self.trg_args.trg_col_clos_price] = data_frame\
            .sort_values(by=[self.src_args.src_col_time])\
            .groupby([
                    self.src_args.src_col_isin,
                    self.src_args.src_col_date
                    ])[self.src_args.src_col_start_price]\
            .transform('last')

        # Renaming columns
        data_frame.rename(columns={
            self.src_args.src_col_min_price: self.trg_args.trg_col_min_price,
            self.src_args.src_col_max_price: self.trg_args.trg_col_max_price,
            self.src_args.src_col_traded_vol: self.trg_args.trg_col_dail_trad_vol
            }, inplace=True)

        # Aggregating per ISIN and day -> opening price, closing price,
        # minimum price, maximum price, traded volume
        data_frame = data_frame.groupby([
            self.src_args.src_col_isin,
            self.src_args.src_col_date], as_index=False)\
            .agg({
                    self.trg_args.trg_col_op_price: 'min',
                    self.trg_args.trg_col_clos_price: 'min',
                    self.trg_args.trg_col_min_price: 'min',
                    self.trg_args.trg_col_max_price: 'max',
                    self.trg_args.trg_col_dail_trad_vol: 'sum'})

        # Change of current day's closing price compared to the
        # previous trading day's closing price in %
        data_frame[self.trg_args.trg_col_ch_prev_clos] = data_frame\
            .sort_values(by=[self.src_args.src_col_date])\
            .groupby([self.src_args.src_col_isin])[self.trg_args.trg_col_op_price]\
            .shift(1)
        data_frame[self.trg_args.trg_col_ch_prev_clos] = (
            data_frame[self.trg_args.trg_col_op_price] \
            - data_frame[self.trg_args.trg_col_ch_prev_clos]
            ) / data_frame[self.trg_args.trg_col_ch_prev_clos ] * 100

        # Rounding to 2 decimals
        data_frame = data_frame.round(decimals=2)

        # Removing the day before extract_date
        data_frame = data_frame[data_frame.Date >= self.extract_date].reset_index(drop=True)
        self._logger.info('Applying transformations to Xetra source data finished...')
        return data_frame

    def load(self, data_frame: pd.DataFrame):
        """
        Saves a Pandas DataFrame to the target

        :param data_frame: Pandas DataFrame as Input
        """

        # Creating target key
        target_key = (
            f'{self.trg_args.trg_key}'
            f'{datetime.today().strftime( self.trg_args.trg_key_date_format )}.'
            f'{self.trg_args.trg_format}'
        )

        # Writing to target
        self.s3_bucket_trg.write_df_to_s3_bucket(data_frame, target_key, self.trg_args.trg_format)
        self._logger.info('Xetra target data successfully written.')

        # Updating meta file
        MetaProcess.update_meta_file(self.meta_update_list, self.meta_key, self.s3_bucket_trg)
        self._logger.info('Xetra meta file successfully updated.')
        return True

    def etl_report1(self):
        """
        Extract, transform and load to create report 1
        """

        # Extraction
        data_frame = self.extract()

        # Transformation
        data_frame = self.transform_report1(data_frame)

        # Load
        self.load(data_frame)
        return True

