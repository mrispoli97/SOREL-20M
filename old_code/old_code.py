from database.sorel20m_db_reader import Dataset
from utils import utils

METADB_PATH = r"C:\Users\mario\Downloads\meta.db"
EMBER_METADB_PATH = r"C:\Users\mario\Downloads\meta.mdb"
DB_CSV_PATH = r"C:\Users\mario\Documents\GitHub\SOREL-20M\database\sorel20m.csv"
DATABASE_DIR_PATH = r"C:\Users\mario\Documents\GitHub\SOREL-20M\database"

TAGS = ["benign", "adware", "flooder", "ransomware", "dropper", "spyware",
        "packed", "crypto_miner", "file_infector", "installer", "worm", "downloader"]

NUM_SAMPLES = 3
MODE = 'train'


def select_samples():
    shas_missing_ember_features = utils.load(
        r"C:\Users\mario\Documents\GitHub\SOREL-20M\shas_missing_ember_features.json", type='json')
    dataset = Dataset(metadb_path=METADB_PATH, tags=TAGS)
    exclude = shas_missing_ember_features
    exclude = []
    for tag in TAGS:
        results = dataset.select(tag=tag, exclude=exclude, mode=MODE, n_samples=NUM_SAMPLES,
                                 verbose=True)
        if len(results) < NUM_SAMPLES:
            print(f"WARNING: retrieved {len(results)} only for {tag}. {NUM_SAMPLES} required.")
        print(results)
        # mode_dir_path = os.path.join(DATABASE_DIR_PATH, MODE)
        # if not os.path.exists(mode_dir_path):
        #     os.mkdir(mode_dir_path)
        # results_dir_path = os.path.join(mode_dir_path, tag)
        # if not os.path.exists(results_dir_path):
        #     os.mkdir(results_dir_path)
        #
        # dest_tag_file_path = os.path.join(results_dir_path, tag + ".json")
        # utils.save(dest_tag_file_path, {
        #     'num samples': len(results),
        #     'samples': results        # }, type='json')


def get_samples_from_mode(mode):
    missing_ember_features = utils.load(r"C:\Users\mario\Documents\GitHub\SOREL-20M\shas_missing_ember_features.json",
                                        type='json')
    dataset = Dataset(metadb_path=METADB_PATH, tags=TAGS)
    files = dataset.get_files_from_mode(mode=mode, exclude=missing_ember_features)
    sha256s = [file[0] for file in files]
    return sha256s


def compare():
    sha256_with_features = utils.load(
        r"C:\Users\mario\PycharmProjects\EmberSorelChecker\sha256 with ember features.json", type='json')
    num_with_features = len(sha256_with_features)
    num_processed = 0
    num_found = 0
    num_not_found = 0
    errors = 0

    sha256_shared = []
    dataset = Dataset(metadb_path=METADB_PATH, tags=TAGS)

    for sha256 in sha256_with_features:
        try:
            file = dataset.get_file(sha256=sha256)
            if file:
                num_found += 1
                sha256_shared.append(file)
            else:
                num_not_found += 1
            print(f"processing ... {num_processed + 1}/{num_with_features}")
        except Exception as e:
            print(e)
            errors += 1
        finally:
            num_processed += 1
    utils.save('sha256 with features present in sorel.json', data={
        'num shared': num_found,
        'num not shared': num_not_found,
        'shared': sha256_shared,
        'num processed': num_processed,
    }, type='json')


def get_file():
    dataset = Dataset(metadb_path=METADB_PATH, tags=TAGS)
    file = dataset.get_file(sha256='0c99777074c56386fc1ddeeb434cd309140c3e30e638c68d385e1523a5013031')
    print(file)
