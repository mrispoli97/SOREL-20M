import os
import json
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import multiprocessing as mp
from utils import utils
import progressbar


def store_results(result):
    mode = result['mode']
    family = result['family']
    num_samples_downlaoded = result['num samples downloaded']
    num_errors = result['num errors']
    num_skipped = result['num skipped']
    report = {
        'mode': mode,
        'num samples downloaded': num_samples_downlaoded,
        'num errors': num_errors,
        'num skipped': num_skipped,
    }
    log_mode_path = os.path.join(LOG_DIR, mode)
    if not os.path.exists:
        os.mkdir(log_mode_path)
    log_path = os.path.join(log_mode_path, family)
    if not os.path.exists:
        os.mkdir(log_path)
    filepath = os.path.join(log_path, mode + " " + family + " - report.json")
    print(f"STORING RESULTS [{mode}][{family}] to {filepath}")
    utils.save(filepath=filepath, data=report, type='json')


def download_dataset(mode, max_samples=None, verbose=False):
    if mode != 'train' and mode != 'validation' and mode != 'test':
        raise ValueError(f"mode {mode} invalid.")
    mode_dir_path = os.path.join(DOWNLOAD_DIR, mode)
    if not os.path.exists(mode_dir_path):
        os.mkdir(mode_dir_path)
    dataset_dir = os.path.join(DATABASE_DIR, mode)
    families = os.listdir(dataset_dir)

    # mp.cpu_count() - 1 if mp.cpu_count() > 1 else 1

    with mp.Pool(mp.cpu_count() - 1 if mp.cpu_count() > 1 else 1) as pool:
        result = pool.map_async(download_files, [(mode, family, verbose, max_samples) for family in families])
        store_results(result=result.get())


def download_single_file(client, filename, dest_dir_path, verbose=False):
    bucket = "sorel-20m"  # substitute your actual bucket name
    path = "09-DEC-2020/binaries" + "/" + filename
    dest_filename = os.path.join(dest_dir_path, filename)
    client.download_file(bucket, path, dest_filename)
    if verbose:
        print(f"downloaded file: {dest_filename}")


def download_files(record):
    mode = record[0]
    family = record[1]
    verbose = record[2]
    max_samples = record[3]

    if verbose:
        print(f"[{mode}][{family}] starting... ")
    my_config = Config(
        region_name='us-west-2',
        signature_version=UNSIGNED,
    )
    client = boto3.client("s3", config=my_config)
    json_file = os.path.join(DATABASE_DIR, mode, family, family + ".json")
    file = open(json_file)
    data = json.load(file)
    samples = set(data['samples'])
    file.close()

    family_dir_path = os.path.join(DOWNLOAD_DIR, mode, family)
    if os.path.exists(family_dir_path):
        samples_already_downloaded = set(os.listdir(family_dir_path))
        samples -= samples_already_downloaded
        skipped = len(samples_already_downloaded)

        if verbose:
            print("------------------------------------------------------")
            print(f"Skipping {skipped} samples for [{mode}][{family}]... found in {family_dir_path}")
            print("------------------------------------------------------")
    else:
        skipped = 0
    num_samples_processed = 0
    num_samples_downloaded = 0
    errors = 0
    num_samples = len(samples)

    dest_dir_path = os.path.join(DOWNLOAD_DIR, mode, family)
    if not os.path.exists(dest_dir_path):
        print(f"Creating dir: {dest_dir_path}")
        os.mkdir(dest_dir_path)

    for sample in samples:
        if num_samples_downloaded + skipped >= max_samples:
            break

        try:
            download_single_file(client=client, filename=sample, dest_dir_path=dest_dir_path, verbose=False)
            num_samples_downloaded += 1
        except Exception as e:
            if family != 'benign':
                print(e)
            errors += 1
        finally:
            num_samples_processed += 1
            if verbose and num_samples_processed % 1000 == 0:
                print(f"[{mode}][{family}] processing... {num_samples_processed}/{num_samples}")
    if verbose:
        print(f"[{mode}][{family}] OK.")

    result = {
        'mode': mode,
        'family': family,
        'num skipped': skipped,
        'num samples downloaded': num_samples_downloaded,
        'num errors': errors
    }
    return result


def download_binaries():
    BASE_DIR = r"/user/mrispoli/datasets/sorel-20m"
    LOG_DIR = os.path.join(BASE_DIR, 'log')
    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)
    DATABASE_DIR = os.path.join(BASE_DIR, 'selected', 'database')
    DOWNLOAD_DIR = r"/mnt/smb/mrispoli/sorel-20m"

    samples_downloaded = {}
    errors = {}
    for mode in ['train', 'validation', 'test']:
        download_dataset(mode=mode, max_samples=3000, verbose=True)


class ProgressPercentage(object):
    def __init__(self, client, bucket, filename):
        # ... everything else the same
        self._size = client.head_object(Bucket=bucket, Key=filename).ContentLength

    # ...


# If you still have the client object you could pass that directly
# instead of transfer._manager._client


def download_ember_features():
    my_config = Config(
        region_name='us-west-2',
        signature_version=UNSIGNED,
    )
    client = boto3.client("s3", config=my_config)
    dest_dir_path = r'../samples_with_features'
    bucket = "sorel-20m"
    prefix = "09-DEC-2020/processed-data/samples_with_features/"
    filename = 'data.mdb'
    dest_filename = os.path.join(dest_dir_path, filename)

    # client.download_file(bucket, prefix + filename, dest_filename)
    statinfo = os.stat(dest_filename)

    up_progress = progressbar.progressbar.ProgressBar(maxval=statinfo.st_size)

    up_progress.start()

    def upload_progress(chunk):
        up_progress.update(up_progress.currval + chunk)

    client.upload_file(filename, bucket, prefix, Callback=upload_progress)

    up_progress.finish()


if __name__ == '__main__':
    pass
