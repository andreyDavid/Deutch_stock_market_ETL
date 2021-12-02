import argparse
import logging
import logging.config
import yaml

from xetra.common.s3 import S3BucketConnector
from xetra.transformers.xetra_transformers import XetraETL, XetraSourceConfig, XetraTargetConfig


def main():
    # Parse YAML file
    # parser = argparse.ArgumentParser(description='Run xetra ETL job')
    # parser.add_argument('config', help='A configuration file in YAML format')
    # args = parser.parse_args()
    config_path = '/Users/andreydavidov/pythonProject/prod_ETL/configs/xetra_report1_config.yaml'
    config = yaml.safe_load(open(config_path))

    # Configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is a test.")

    # configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)

    # Reading s3 configuration
    s3_config = config['s3']

    # Creating the S3BucketConnerctor class instance for source and target
    s3_bucket_src = S3BucketConnector(profile_name=s3_config['profile_name'],
                                      end_point_url=s3_config['src_endpoint_url'],
                                      bucket=s3_config['src_bucket'])

    s3_bucket_target = S3BucketConnector(profile_name=s3_config['profile_name'],
                                         end_point_url=s3_config['trg_endpoint_url'],
                                         bucket=s3_config['trg_bucket'])

    # Reading source configuration
    source_config = XetraSourceConfig(**config['source'])

    # Reading target configuration
    target_config = XetraTargetConfig(**config['target'])

    # Reading meta file configuration
    meta_config = config['meta']

    # Creating XetraETL class instance
    logger.info('Xetra ETL job started')
    xetra_etl = XetraETL(s3_bucket_src, s3_bucket_target,
                         meta_config['meta_key'], source_config, target_config)

    # creating ETL job for Xetra report 1
    xetra_etl.etl_report1()
    logger.info('Xetra ETL job finished.')


if __name__ == '__main__':
    main()
