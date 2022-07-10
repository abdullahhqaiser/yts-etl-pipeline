from common import YTS_Transformer
import yaml
import argparse
import logging
import sys


def main():
    parser = argparse.ArgumentParser(description='run etl job')
    parser.add_argument('config')
    args = parser.parse_args()

    config = yaml.safe_load(open(args.config))

    # LOADING SOURCE CONFIGS.

    source_config = config['source']

    # LOADING TRACKING CONFIG
    tracking_config = config['tracking']
    # LOADING LOGGING CONFIG
    logging_config = config['logging']

    # LOADING DESTINATION CONFIG
    destination_config = config['destination']

    logging.basicConfig(level=logging_config['level'], format=logging_config['format'],
                        handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler(sys.stdout)
    ])
    logger = logging.getLogger(__name__)

    logger.info('this is test')

    etl = YTS_Transformer.etl(
        source_config, tracking_config, destination_config)

    etl.load()

if __name__ == "__main__":
    main()
