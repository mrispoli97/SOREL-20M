import boto3
from botocore import UNSIGNED
from botocore.config import Config
import os
import config as cfg

class Downloader:

    def __init__(self, config=None):
        self._init(config)

    def _init(self, config):
        self._config = config if config else Config(
            region_name='us-west-2',
            signature_version=UNSIGNED,
        )
        self._client = boto3.client("s3", config=self._config)
        self._bucket = "sorel-20m"
        self._prefix = "09-DEC-2020/binaries"

    def _get_progress(self, current_step, num_steps):
        return int(current_step / num_steps * 100)

    def _debug_doc(self):
        import boto3
        import botocore

        filename = '020cafa9f87cd60f427ba751f2d55ad4ee4378c3cc7cdaf01d05464218f62d33'
        dst_dir = os.path.join(cfg.BASE_DIR, 'dataset', 'file_infector')
        s3 = boto3.resource('s3')

        try:
            s3.Bucket(self._bucket).download_file(self._prefix+'/'+filename, os.path.join(dst_dir, filename))
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

    def _debug(self):
        # objects = self._client.list_objects_v2(Bucket=self._bucket, Prefix=self._prefix)
        # for obj in objects['Contents']:
        #     print(obj['Key'])

        filename = '252f1ef78fa9490b04704c0527bde9ed3a361bdd700c432309996cbcbd23fe19'
        paginator = self._client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=self._bucket)

        found = False
        for page in page_iterator:
            if page['KeyCount'] > 0:
                for item in page['Contents']:
                    key = item['Key']
                    if filename in key:
                        found = True
                        print(f"Found {filename}")
                    if found:
                        break
            if found:
                break

    def download_files(self, dict, dst, verbose=True):
        if not os.path.exists(dst):
            os.makedirs(dst)

        errors = []
        num_downloaded_successfully = 0
        num_files_to_download = sum([len(samples) for samples in dict.values()])
        num_processed = 0
        progress_percentage = self._get_progress(num_processed, num_files_to_download)
        if verbose:
            print(f"Downloading ... {progress_percentage}%")
        for family, files in dict.items():
            dir_path = os.path.join(dst, family)
            if not os.path.exists(dir_path):
                os.mkdir(dir_path)
            for file in files:
                filepath = self.download_file(file, dst=dir_path, verbose=False)
                num_processed += 1
                if filepath is not None:
                    num_downloaded_successfully += 1
                else:
                    errors.append(file)
                new_progress_percentage = self._get_progress(num_processed, num_files_to_download)
                if verbose and new_progress_percentage > progress_percentage:
                    progress_percentage = new_progress_percentage
                    print(f"Downloading ... {progress_percentage}%")
        return {
            'num downloaded successfully': num_downloaded_successfully,
            'num to download': num_files_to_download,
            'errors': errors,
        }

    def download_file(self, filename, dst, verbose=False):
        dst_filepath = os.path.join(dst, filename)
        try:
            self._client.download_file(self._bucket, self._prefix+"/"+filename, dst_filepath)
            if verbose:
                print(f"{filename} downloaded successfully in {dst}")
            return dst_filepath
        except Exception as e:
            print(f"An exeption occurred while downloading {filename} in {dst}: {e}")
            return None
