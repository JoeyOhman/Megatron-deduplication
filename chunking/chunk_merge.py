import argparse
import json
from collections import defaultdict
from typing import Dict, List

from read_work_write_chunk_merge import read_work_write


ROOT_IN = ""

NOT_SUPPORTED_LANG_CODE = "non_supported_lang"
lang_dirs = ["en", "no", "da", "sv", "is", "fo", NOT_SUPPORTED_LANG_CODE, "dirty", "familjeliv", "code"]


standard_keys = ["md5", "filename", "keep", "filters", "lang", "len_char", "len_utf8bytes", "len_words",
                 "len_sents", "text"]

banned_forums = {
    "Samhälle/Aktuella brott och kriminalfall",
    "Samhälle/Brott och brottsbekämpning",
    "Samhälle/Försvunna personer",
    "Samhälle/Organiserad brottslighet",
    "Övrigt/Arkiverade forum/Antisemitism, sionism och judiska maktföhållanden",
    "Övrigt/Arkiverade forum/Kriminella nätverk och brottslighet [arkiverad version]"
}


def work_func(line):
    obj = json.loads(line)
    try:

        # Remove if bad flashback forum
        fb_forum = obj.get("forum", None)
        if fb_forum is not None:
            forum_path = "/".join(fb_forum)
            if forum_path in banned_forums:
                print("REMOVING FORUM")
                obj["keep"] = 0
                obj["filters"].append("bad_flashback_forum")

        # Remove all non-standard json keys
        obj = {
            k: obj[k] for k in standard_keys
        }

        metrics = (obj["len_utf8bytes"], obj["len_char"])
        return json.dumps(obj, ensure_ascii=False), obj["keep"], metrics
    except KeyError:
        return json.dumps(obj, ensure_ascii=False), None, None

    # return line, True


# Returns a dict str -> list[str]
def get_output_to_inputs_mapping(input_root_dir: str, output_root_dir: str, input_files: List[str]
                                 ) -> Dict[str, List[str]]:

    output_file_to_in_files = defaultdict(list)

    for input_path in input_files:

        in_path_no_root = input_path.replace(input_root_dir, "")
        slash_split = [el for el in in_path_no_root.split("/") if el]

        # TODO: Add all directories here, e.g. famlife
        while slash_split[0] in lang_dirs or "chunk_" in slash_split[0]:
            slash_split = slash_split[1:]

        file_path_id = "/".join(slash_split)
        out_path = output_root_dir + "/" + file_path_id
        output_file_to_in_files[out_path].append(input_path)

    return output_file_to_in_files


def main(args):
    global ROOT_IN
    ROOT_IN = args.input_root_dir
    print("Merging files")
    print("Reading files from:", args.input_root_dir)
    print("Writing files to:", args.output_root_dir)
    # print(args.input_files[0])
    output_file_to_in_files = get_output_to_inputs_mapping(args.input_root_dir, args.output_root_dir, args.input_files)
    for output_file, input_files in output_file_to_in_files.items():
        # print(f"{output_file}: {input_files}\n")
        read_work_write(input_files, output_file, work_func, args.output_root_dir, args.num_workers)


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
    parser.add_argument('--num_workers', type=int, default=8,
                        help='The number of CPU workers to process each line with.')
    args = parser.parse_args()

    assert args.input_root_dir != args.output_root_dir

    main(args)
