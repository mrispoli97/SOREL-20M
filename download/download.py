import boto3
from botocore import UNSIGNED
from botocore.config import Config
import os


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
            if not os.path.exists(dst_filepath):
                self._client.download_file(self._bucket, self._prefix + "/" + filename, dst_filepath)
            if verbose:
                print(f"{filename} downloaded successfully in {dst}")
            return dst_filepath
        except Exception as e:
            print(f"An exeption occurred while downloading {filename} in {dst}: {e}")
            return None
