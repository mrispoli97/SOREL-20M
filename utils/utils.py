import pickle
import csv
import json


def save(filepath, data, type, verbose=False):
    if verbose:
        print(f"Saving {len(data)} items in {filepath}")

    if type == 'pickle':
        with open(filepath, 'wb') as handle:
            pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)
    elif type == 'csv':

        with open(filepath, mode='w', newline='') as file:
            csv_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            line_count = 0
            for item in data:
                if line_count == 0:
                    header = item.keys()
                    csv_writer.writerow(list(header))
                else:
                    csv_writer.writerow(item.values())
                line_count += 1
    elif type == 'json':
        with open(filepath, 'w') as json_file:
            json.dump(data, json_file, indent=4)
    else:
        raise ValueError(f"type {type} is not supported.")


def load(filepath, type, verbose=False):
    if verbose:
        print(f"Loading data from {filepath}")

    if type == 'pickle':
        with open(filepath, 'rb') as handle:
            data = pickle.load(handle)
            return data
    elif type == 'csv':
        with open(filepath, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            line_count = 0
            data = []
            for row in csv_reader:
                if line_count == 0:
                    header = row
                else:
                    values = row
                    for i in range(0, len(header)):
                        tag = header[i]
                        value = values[i]
                        item = {
                            tag: value
                        }
                        data.append(item)
                line_count += 1
            return data
    elif type == 'json':
        with open(filepath) as f:
            data = json.load(f)
            return data
    else:
        raise ValueError(f"type {type} is not supported.")
