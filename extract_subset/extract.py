import argparse

from read_work_write_extract import read_work_write


def main(args):
    read_work_write(args.input_files, args.output_file, args.root_in)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_file', type=str, default=None,
                        help='Output jsonl file')
    parser.add_argument('--input_files', nargs='*', default=None,
                        help='List of the input files, e.g. --input_files asd.jsonl qwe.jsonl')
    parser.add_argument('--root_in', type=str, default=None,
                        help='Root path to input files')
    args = parser.parse_args()
    main(args)
