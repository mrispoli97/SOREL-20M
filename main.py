from download.download import Downloader
from utils import utils
import config as cfg
import os
from datetime import datetime
import argparse


def save_report(report):
    report_dir = os.path.join(cfg.BASE_DIR, 'download', 'report', datetime.now().strftime("%d_%m_%Y %H_%M_%S"))
    os.makedirs(report_dir)
    utils.save(os.path.join(report_dir, 'report.json'), data=report, type='json')


def download_files():
    parser = argparse.ArgumentParser()
    parser.add_argument('--samples', help='samples to download [json] family: samples', required=False)
    parser.add_argument('--dst', help='dir path to store files downloaded', required=False)
    parser.add_argument('--status_dst', help='Do the bar option', required=True)

    args = vars(parser.parse_args())
    selected_samples_filepath = args['samples'] if 'samples' in args \
        else os.path.join(cfg.BASE_DIR,
                          'samples_with_both_features_and_binaries',
                          'selected - 100.json')

    dst_filepath = args['dst'] if 'dst' in args else os.path.join(cfg.BASE_DIR, 'dataset')
    selected_samples = utils.load(selected_samples_filepath, type='json')
    downloader = Downloader()
    report = downloader.download_files(selected_samples, dst=dst_filepath, verbose=True)
    save_report(report)


if __name__ == '__main__':
    download_files()
