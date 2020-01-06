import argparse
import csv


def _get_batches(source_list: list, batch_size: int):
    for i in range(0, len(source_list), batch_size):
        yield source_list[i:i + batch_size]


def _split_csv(path_to_csv: str, batch_size: int):
    with open(path_to_csv) as csv_file:
        csv_rows = [row for row in csv.DictReader(csv_file)]

    for csv_batch in _get_batches(csv_rows, batch_size):
        yield csv_batch


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Split csv file into batches and save them')
    parser.add_argument(
        '--csv', metavar='path', dest='path_to_csv', type=str, required=True,
        help='path to input csv file')
    parser.add_argument(
        '--batch-size', metavar='number', dest='batch_size', type=int, required=True,
        help='max number of rows in output csv files')
    args = parser.parse_args()

    for i, batch in enumerate(_split_csv(args.path_to_csv, args.batch_size), 1):
        with open(f'{args.path_to_csv}.{i}', 'w') as f:
            cw = csv.DictWriter(f, fieldnames=list(batch[0].keys()))
            cw.writeheader()
            cw.writerows(batch)
