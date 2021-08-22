import lmdb
import msgpack
import zlib
import numpy as np


class FeatureDatabase(object):

    def __init__(self, path, postproc_func=None):
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
                        features = msgpack.loads(zlib.decompress(item), strict_map_key=False)
                        data[sha256] = features
                    num_items_processed += 1
                    print(f"processing: {num_items_processed}/{num_items}")
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