import os
from utils import utils as u
from binary_database import BinaryDatabase
import config as cfg


def save_sha256_for_samples_with_both_binary_and_features():
    samples_with_features_dir = r"/samples_with_features"
    samples_with_both_features_and_binaries_dir = r"/samples_with_both_features_and_binaries"
    DB_path = r"C:\Users\mario\Downloads\meta.db"
    dataset = BinaryDatabase(DB_path)
    data = u.load(os.path.join(samples_with_features_dir, 'samples_with_features.json'), type='json')
    num_samples = data['size']
    samples = data['sha256']

    result = {}
    processed_samples = 0
    old_percentage_of_progress = 0
    print(f"processing sample {old_percentage_of_progress} %")
    for sha256 in samples:
        new_percentage_of_progress = int((processed_samples + 1) / num_samples * 100)
        if new_percentage_of_progress > old_percentage_of_progress:
            old_percentage_of_progress = new_percentage_of_progress
            print(f"processing sample {old_percentage_of_progress} %")
        file = dataset.get_file(sha256)
        if file:
            is_malware = file['is_malware']
            scores = {}
            if is_malware:
                scores['adware'] = file['adware']
                scores['crypto_miner'] = file['crypto_miner']
                scores['downloader'] = file['downloader']
                scores['dropper'] = file['dropper']
                scores['file_infector'] = file['file_infector']
                scores['flooder'] = file['flooder']
                scores['installer'] = file['installer']
                scores['packed'] = file['packed']
                scores['ransomware'] = file['ransomware']
                scores['spyware'] = file['spyware']
                scores['worm'] = file['worm']

                best_2 = list({k: v for k, v in sorted(scores.items(), key=lambda item: -item[1])})[:2]
                family_1 = best_2[0]
                family_2 = best_2[1]
                threshold = scores[family_1] - scores[family_2]
                if scores[family_1] > 0:
                    result[sha256] = {
                        'is_malware': True,
                        'family': family_1,
                        'threshold': threshold
                    }
                else:
                    result[sha256] = {
                        'is_malware': False
                    }
            else:
                result[sha256] = {
                    'is_malware': False
                }
        processed_samples += 1

    u.save(os.path.join(samples_with_both_features_and_binaries_dir, "samples_with_both_features_and_binaries.json"),
           data={
               'size': len(result),
               'samples': result,

           }, type='json')


def sort_malwares_by_threshold():
    samples_with_both_features_and_binaries_dir = r"/samples_with_both_features_and_binaries"
    samples_with_both_features_and_binaries = u.load(
        os.path.join(samples_with_both_features_and_binaries_dir, "samples_with_both_features_and_binaries.json"),
        type='json')
    size = samples_with_both_features_and_binaries['size']
    samples = samples_with_both_features_and_binaries['samples']
    malware_samples = {}
    num_processed = 0
    percentage = int(num_processed / size * 100)
    for sha256, sample in samples.items():
        is_malware = sample['is_malware']
        if is_malware:
            family = sample['family']
            threshold = sample['threshold']
            if family not in malware_samples:
                malware_samples[family] = []
            malware_samples[family].append({
                'sha256': sha256,
                'threshold': threshold,
            })
        num_processed += 1
        new_percentage = int(num_processed / size * 100)
        if new_percentage > percentage:
            percentage = new_percentage
            print(f"Processing.. {percentage}%")

    num_sorted = 0
    num_to_sort = len(malware_samples)
    for family, samples in malware_samples.items():
        print(f"Sorting ...{num_sorted + 1}/{num_to_sort}")
        malware_samples[family] = sorted(samples, key=lambda k: -k['threshold'])
        num_sorted += 1
    u.save(os.path.join(samples_with_both_features_and_binaries_dir, 'family_sample_threshold.json'),
           data=malware_samples, type='json')


def select(max=100):
    samples_with_both_features_and_binaries_dir = os.path.join(cfg.BASE_DIR, "samples_with_both_features_and_binaries")
    data = u.load(os.path.join(samples_with_both_features_and_binaries_dir, 'family_sample_threshold.json'),
                  type='json')
    selected = {}
    for family, samples in data.items():
        selected[family] = [sample['sha256'] for sample in samples[:max]]

    u.save(os.path.join(samples_with_both_features_and_binaries_dir, 'selected - ' + str(max) + '.json'), data=selected,
           type='json')
    print("Saved")


def get_progress_percentage(step, num_steps):
    return int(step / num_steps * 100) if num_steps > 0 else 100


if __name__ == '__main__':
    select(max=6899)
