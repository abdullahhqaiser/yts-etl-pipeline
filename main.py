from common import YTS_Transformer
import yaml
import argparse


def main():
    parser = argparse.ArgumentParser(description='run etl job')
    parser.add_argument('config')
    args = parser.parse_args()

    config = yaml.safe_load(open(args.config))

    print(config)
    


if __name__ == "__main__":
    main()
