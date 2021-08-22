import sqlite3
import config


#before Dataset(data.Dataset)
class BinaryDatabase:

    def __init__(self, metadb_path, tags=None):

        self.metadb_path = metadb_path
        self.tags = tags if tags else ["benign", "adware", "flooder", "ransomware", "dropper", "spyware",
                                       "packed", "crypto_miner", "file_infector", "installer", "worm", "downloader"]

    def select(self, tag, exclude=[], mode='train', n_samples=None, verbose=False):

        conn = sqlite3.connect(self.metadb_path)
        cur = conn.cursor()
        query = 'select sha256'
        query += " from meta"

        if mode == 'train':
            condition_for_mode = 'rl_fs_t'
        elif mode == 'validation':
            condition_for_mode = '(rl_fs_t >= {}) and (rl_fs_t < {})'.format(config.train_validation_split,
                                                                             config.validation_test_split)
        elif mode == 'test':
            condition_for_mode = 'rl_fs_t >= {}'.format(config.validation_test_split)
        else:
            raise ValueError('invalid mode: {}'.format(mode))

        if tag not in self.tags:
            raise ValueError('invalid tag: {}'.format(tag))

        if tag != 'benign':
            condition_for_tag = '{} > 0'.format(tag)
        else:
            condition_for_tag = 'is_malware == 0'

        query += " where " + condition_for_mode + "" + " and " + "" + condition_for_tag + ""
        if verbose:
            print("QUERY: ", query)
        query += " and " + "sha256 not in ('%s') " % ("','".join(exclude))

        if type(n_samples) != type(None):
            query += ' limit {}'.format(n_samples)

        vals = cur.execute(query).fetchall()
        results = []
        for val in vals:
            results.append(val[0])
        return results

    def get_file(self, sha256):
        conn = sqlite3.connect(self.metadb_path)
        cur = conn.cursor()

        query = "select * from meta where sha256 == '" + sha256 + "';"
        # print(query)
        val = cur.execute(query).fetchone()
        header = [name[0] for name in cur.description]
        data = {}
        if val:
            for i in range(0, len(header)):
                data[header[i]] = val[i]
            return data
        return None

    def get_files_from_mode(self, mode, exclude=[], limit=None):
        if mode == 'train':
            condition_for_mode = 'rl_fs_t < {}'.format(config.train_validation_split)
        elif mode == 'validation':
            condition_for_mode = '(rl_fs_t >= {}) and (rl_fs_t < {})'.format(config.train_validation_split,
                                                                             config.validation_test_split)
        elif mode == 'test':
            condition_for_mode = 'rl_fs_t >= {}'.format(config.validation_test_split)

        if exclude:
            exclude_condition = "sha256 not in ('%s') " % ("','".join(exclude))

        if limit:
            condition_for_limit = ' limit {}'.format(limit)

        query = "select sha256 from meta where " + condition_for_mode

        if exclude:
            query += " and " + exclude_condition

        if limit:
            query += condition_for_limit

        query += ';'
        conn = sqlite3.connect(self.metadb_path)
        cur = conn.cursor()
        vals = cur.execute(query).fetchall()
        return vals