import argparse
from collections import defaultdict
from typing import Dict, List

from read_work_write_chunk_merge import read_work_write


ROOT_IN = ""

NOT_SUPPORTED_LANG_CODE = "non_supported_lang"
lang_dirs = ["en", "no", "da", "sv", "is", "fo", NOT_SUPPORTED_LANG_CODE]


def work_func(line):
    return line


# Returns a dict str -> list[str]
def get_output_to_inputs_mapping(input_root_dir: str, output_root_dir: str, input_files: List[str]
                                 ) -> Dict[str, List[str]]:

    output_file_to_in_files = defaultdict(list)

    for input_path in input_files:

        in_path_no_root = input_path.replace(input_root_dir, "")
        slash_split = [el for el in in_path_no_root.split("/") if el]

        while slash_split[0] in lang_dirs or "chunk_" in slash_split[0]:
            slash_split = slash_split[1:]

        file_path_id = "/".join(slash_split)
        out_path = output_root_dir + "/" + file_path_id
        output_file_to_in_files[out_path].append(input_path)

    return output_file_to_in_files


def main(args):
    global ROOT_IN
    ROOT_IN = args.input_root_dir
    print(args.input_root_dir)
    print(args.output_root_dir)
    # print(args.input_files[0])
    output_file_to_in_files = get_output_to_inputs_mapping(args.input_root_dir, args.output_root_dir, args.input_files)
    for output_file, input_files in output_file_to_in_files.items():
        # print(f"{output_file}: {input_files}\n")
        read_work_write(input_files, output_file, work_func, args.input_root_dir)



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
    args = parser.parse_args()

    assert args.input_root_dir != args.output_root_dir

    main(args)
