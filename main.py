from download.download import Downloader
from utils import utils
import config as cfg
import os
from datetime import datetime


def save_report(report):
    report_dir = os.path.join(cfg.BASE_DIR, 'download', 'report', datetime.now().strftime("%d_%m_%Y %H_%M_%S"))
    os.makedirs(report_dir)
    utils.save(os.path.join(report_dir, 'report.json'), data=report, type='json')


if __name__ == '__main__':
    selected_samples_filepath = os.path.join(cfg.BASE_DIR, 'samples_with_both_features_and_binaries', 'selected.json')
    selected_samples = utils.load(selected_samples_filepath, type='json')
    downloader = Downloader()
    report = downloader.download_files(selected_samples, dst=os.path.join(cfg.BASE_DIR, 'dataset'), verbose=True)
    save_report(report)
