import lmdb
import msgpack
import zlib
import numpy as np
import config as cfg
from pprint import pprint
from utils import utils as u
import db_utils as dbu
import os
import argparse


class FeatureDatabase(object):

    def __init__(self, path=None, postproc_func=None):
        path = path if path else cfg.FEATURES_DATABASE
        self.env = lmdb.open(path, subdir=False,
                             readonly=True, lock=False,
                             readahead=False, meminit=False)
        self.postproc_func = postproc_func

    def __call__(self, key):
        with self.env.begin() as txn:
            x = txn.get(key.encode('ascii'))
        if x is None: return None
        x = msgpack.loads(zlib.decompress(x), strict_map_key=False)
        if self.postproc_func is not None:
            x = self.postproc_func(x)
        return x

    def get_keys(self):
        print("Getting keys...")
        with self.env.begin(write=False) as txn:
            d = txn.stat()
            num_keys = d['entries']
            num_keys_processed = 0
            percent_of_progress = int(num_keys_processed / num_keys * 100)
            print(f"Processing ... {percent_of_progress}%")
            with txn.cursor() as curs:
                sha256 = []
                for key, _ in curs:
                    sha256.append(key.decode('utf-8'))
                    new_percent_of_progress = int(num_keys_processed / num_keys * 100)
                    if new_percent_of_progress > percent_of_progress:
                        percent_of_progress = new_percent_of_progress
                        print(f"Processing ... {percent_of_progress}%")
                    num_keys_processed += 1
                return sha256

    def get_data(self):
        data = {}
        with self.env.begin(write=False) as txn:
            d = txn.stat()
            num_items = d['entries']
            with txn.cursor() as curs:
                num_items_processed = 0
                print(f"num items: {num_items}")
                for key, item in curs:
                    sha256 = key.decode('utf-8')
                    if item:
                        features = msgpack.loads(zlib.decompress(item), strict_map_key=False)[0]
                        data[sha256] = features
                    num_items_processed += 1
                    print(f"processing: {num_items_processed}/{num_items}")
        return data

    def get_files(self, sha256_to_get):
        sha256_missing = sha256_to_get.copy()
        data = {}

        with self.env.begin(write=False) as txn:
            d = txn.stat()
            num_items = d['entries']
            with txn.cursor() as curs:
                num_items_processed = 0
                print(f"num items: {num_items}")
                percentage = dbu.get_progress_percentage(num_items_processed, num_items)
                print(f"processing: {percentage}%")
                for key, item in curs:
                    sha256 = key.decode('utf-8')
                    if item and sha256 in sha256_missing.copy():
                        features = msgpack.loads(zlib.decompress(item), strict_map_key=False)[0]
                        data[sha256] = features
                        sha256_missing.remove(sha256)
                        if len(sha256_missing) == 0:
                            break

                    num_items_processed += 1
                    new_percentage = dbu.get_progress_percentage(num_items_processed, num_items)
                    if new_percentage > percentage:
                        percentage = new_percentage
                        print(f"processing: {percentage}%")
        return data

    def features_postproc_func(self, x):
        x = np.asarray(x[0], dtype=np.float32)
        lz = x < 0
        gz = x > 0
        x[lz] = - np.log(1 - x[lz])
        x[gz] = np.log(1 + x[gz])
        return x

    def tags_postproc_func(self, x):
        x = list(x[b'labels'].values())
        x = np.asarray(x)
        return x


def get_features_of_selected_samples(dst=None, db_path=None):
    selected_samples = u.load(
        filepath=os.path.join(cfg.BASE_DIR, 'samples_with_both_features_and_binaries', 'selected - 6899.json'),
        type='json'
    )
    sha256s = []
    for family, samples in selected_samples.items():
        for sha256 in samples:
            sha256s.append(sha256)

    db = FeatureDatabase(db_path)
    print("Features extraction ...")
    data = db.get_files(sha256s)

    features_dir = dst if dst else os.path.join(cfg.BASE_DIR, 'features')
    if not os.path.exists(features_dir):
        os.makedirs(features_dir)

    print("Saving features ...")
    for family, samples in selected_samples.items():
        family_dir = os.path.join(features_dir, family)
        if not os.path.exists(family_dir):
            os.makedirs(family_dir)
        features = {}
        for sha256 in samples:
            features[sha256] = data[sha256]
        u.save(filepath=os.path.join(family_dir, family + '.json'), data=features, type='json')
    print("Saved.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dst', help='dst folder path', required=False, default=None)
    parser.add_argument('--db_path', help='db path', required=False, default=None)
    args = vars(parser.parse_args())
    get_features_of_selected_samples(dst=args['dst'], db_path=args['db_path'])
