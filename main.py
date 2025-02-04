from download.download import Downloader
from database.feature_database import get_features_of_selected_samples
from utils import utils
from config import config as cfg
import os
import numpy as np
from datetime import datetime
import argparse
from feature_extraction.feature_extraction import PEFeatureExtractor
from pprint import pprint
import pandas as pd


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


def extract_features():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dst', help='dst folder path', required=False, default=None)
    parser.add_argument('--db_path', help='db path', required=False, default=None)
    args = vars(parser.parse_args())
    get_features_of_selected_samples(dst=args['dst'], db_path=args['db_path'])


def split_into_train_validation_test():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dst', help='dst folder path', required=True)
    parser.add_argument('--src', help='src folder path', required=True)
    parser.add_argument('--train_partition', help='train partition factor', required=False, default=0.7)
    args = vars(parser.parse_args())

    src = args['src']
    dst = args['dst']
    train_partition = args['train_partition']
    families = os.listdir(src)

    num_families = len(families)
    num_families_processed = 0

    for family in families:
        family_dir_path = os.path.join(src, family)
        for mode in ['train', 'validation', 'test']:
            dst_family_dir_path = os.path.join(dst, mode, family)
            if not os.path.exists(dst_family_dir_path):
                os.makedirs(dst_family_dir_path)
        samples = os.listdir(family_dir_path)
        num_samples = len(samples)
        val_partition = (1 - train_partition) / 2
        num_train_samples = int(train_partition * num_samples)
        num_val_samples = int(val_partition * num_samples)
        train_samples = samples[:num_train_samples]
        val_samples = samples[num_train_samples: num_train_samples + num_val_samples]
        test_samples = samples[num_train_samples + num_val_samples:]

        data = {
            'train': train_samples,
            'validation': val_samples,
            'test': test_samples
        }

        for mode, samples in data.items():
            num_samples = len(samples)
            num_samples_processed = 0
            for sample in samples:
                print(
                    f"Processing [{num_families_processed + 1}/{num_families}][{num_samples_processed + 1}/{num_samples}]")
                src_sample = os.path.join(src, family, sample)
                dst_sample = os.path.join(dst, mode, family, sample)
                os.replace(src_sample, dst_sample)
                num_samples_processed += 1

        num_families_processed += 1


def extract_features_from_all():
    parser = argparse.ArgumentParser()
    default_dst = os.path.join(cfg.BASE_DIR, 'features')
    parser.add_argument('--src', help='src folder path', required=True)
    parser.add_argument('--dst', help='dst folder path', required=False, default=default_dst)
    parser.add_argument('--samples', help='samples file path', required=True)
    args = vars(parser.parse_args())
    src = args['src']
    dst = args['dst']
    samples_file_path = args['samples']
    data = utils.load(samples_file_path, type='json')
    extractor = PEFeatureExtractor()
    if not os.path.exists(dst):
        os.makedirs(dst)

    num_families = len(list(data.keys()))
    num_families_processed = 0
    for family, samples in data.items():

        family_dir_path = os.path.join(src, family)
        dst_family_dir_path = os.path.join(dst, family)
        if not os.path.exists(dst_family_dir_path):
            os.mkdir(dst_family_dir_path)
        features = {}

        num_files = len(samples)
        num_files_processed = 0
        percentage = utils.get_percentage(num_files_processed, num_files)
        print(f"[{num_families_processed + 1}/{num_families}] Elaborating features... {percentage}")
        for filename in samples:
            filepath = os.path.join(family_dir_path, filename)
            with open(filepath, 'rb') as f:
                bytes = f.read()
            features[filename] = [str(value) for value in extractor.feature_vector(bytes)]
            num_files_processed += 1
            new_percentage = utils.get_percentage(num_files_processed, num_files)
            if new_percentage > percentage:
                percentage = new_percentage
                print(f"[{num_families_processed + 1}/{num_families}] Elaborating features... {percentage}")

        num_families_processed += 1
        dst_filepath = os.path.join(dst_family_dir_path, family + ".json")
        print(f"saving to {dst_filepath}")
        utils.save(dst_filepath, data=features, type='json')


def extract_features_from_samples_in_directory():
    parser = argparse.ArgumentParser()
    parser.add_argument('--src', help='src folder path', required=True)
    parser.add_argument('--dst', help='dst folder path', required=True)
    args = vars(parser.parse_args())
    src = args['src']
    dst = args['dst']
    features_df_path = os.path.join(dst, 'data.pkl')

    if not os.path.exists(features_df_path):

        dataframe = pd.DataFrame()

        families = os.listdir(src)
        num_families = len(families)
        num_families_processed = 0

        for family in families:
            family_src = os.path.join(src, family)
            features = {}
            samples = os.listdir(family_src)
            num_files = len(samples)
            num_files_processed = 0
            percentage = utils.get_percentage(num_files_processed, num_files)
            extractor = PEFeatureExtractor()
            print(f"[{num_families_processed + 1}/{num_families}] Elaborating features... {percentage}%")
            for sample in samples:
                filepath = os.path.join(family_src, sample)
                with open(filepath, 'rb') as f:
                    bytes = f.read()

                features = [str(value) for value in extractor.feature_vector(bytes)]
                features_df = {'label': family, 'id': sample}
                features_df.update({'feature_' + str(i): float(feature) for i, feature in enumerate(features)})
                dataframe = dataframe.append(features_df, ignore_index=True)
                num_files_processed += 1
                new_percentage = utils.get_percentage(num_files_processed, num_files)
                if new_percentage > percentage:
                    percentage = new_percentage
                    print(f"[{num_families_processed + 1}/{num_families}] Elaborating features... {percentage}%")
            num_families_processed += 1

        if not os.path.exists(dst):
            os.makedirs(dst)
        dataframe.to_pickle(features_df_path)


def count_samples_with_features():
    parser = argparse.ArgumentParser()
    parser.add_argument('--src', help='src folder path', required=True)
    args = vars(parser.parse_args())
    src = args['src']
    families = os.listdir(src)
    for family in families:
        json_data = utils.load(os.path.join(src, family, family + '.json'), type='json')
        print(f"{family}: {len(json_data)}")


if __name__ == '__main__':
    extract_features_from_samples_in_directory()
