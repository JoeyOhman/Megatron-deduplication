import argparse

from read_work_write_chunk_split import read_work_write


ROOT_IN = ""


def work_func(line):
    return line


def main(args):
    global ROOT_IN
    ROOT_IN = args.input_root_dir

    for input_path in args.input_files:
        output_file_path_template = input_path.replace(args.input_root_dir, args.output_root_dir + "/chunk_{idx}")
        read_work_write(input_path, output_file_path_template, work_func, args.input_root_dir, args.num_chunks)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_root_dir', type=str, default=None,
                        help='Input root directory path, input_root_dir will be replaced with output_root_dir to get '
                             'output paths.')
    parser.add_argument('--output_root_dir', type=str, default=None,
                        help='Output root directory path, input_root_dir will be replaced with output_root_dir to get '
                             'output paths')
    parser.add_argument('--input_files', nargs='*', default=None,
                        help='List of the input files, e.g. --input_files asd.jsonl qwe.jsonl')
    parser.add_argument('--num_chunks', type=int, default=None,
                        help='Output root directory path, input_root_dir will be replaced with output_root_dir to get '
                             'output paths')
    args = parser.parse_args()

    assert args.input_root_dir != args.output_root_dir
    assert args.num_chunks is not None

    main(args)
