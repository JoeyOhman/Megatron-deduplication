import os
import argparse
import json
import pickle
from pathlib import Path

from read_work_write_exact_dedup import read_work_write

md5_seen = set()
url_seen = set()

num_total = 0
num_removed_md5 = 0
num_removed_url = 0

lang_dirs = ["en", "no", "da", "sv", "is", "fo"]

ROOT_IN = ""


def deduplicate(json_line):
    global num_total, num_removed_md5, num_removed_url

    num_total += 1
    json_obj = json.loads(json_line)
    lang = json_obj["lang"]
    if lang == "nn":
        lang = "no"

    md5, url = json_obj["md5"], json_obj.get("url", None)
    # if json_obj["keep"] == 0:
        # return json_line, lang

    keep = True

    if md5 in md5_seen:
        json_obj["keep"] = 0
        json_obj["filters"].append("md5_dedup")
        num_removed_md5 += 1
        keep = False

    if url is not None and url in url_seen:
        json_obj["keep"] = 0
        json_obj["filters"].append("url_dedup")
        num_removed_url += 1
        keep = False

    if keep:
        md5_seen.add(md5)
        if url is not None:
            url_seen.add(url)

    return json.dumps(json_obj, ensure_ascii=False), lang


def check_if_file_done(lang_to_file):
    for lang in lang_dirs:
        output_file_path = lang_to_file[lang]
        # If we find an existing output file, this input file is already deduplicated
        if os.path.isfile(output_file_path):
            return True
    return False


def load_persistent(output_root):
    output_pickle_path = output_root + "/exact_dedup_checkpoint.pickle"
    try:
        with open(output_pickle_path, 'rb') as f:
            pickle_dict = pickle.load(f)

        global md5_seen, url_seen, num_total, num_removed_md5, num_removed_url
        md5_seen = pickle_dict["md5_seen"]
        url_seen = pickle_dict["url_seen"]

        num_total = pickle_dict["num_total"]
        num_removed_md5 = pickle_dict["num_removed_md5"]
        num_removed_url = pickle_dict["num_removed_url"]

        print(f"Loaded state from persistent storage: {output_pickle_path}")

    except FileNotFoundError:
        print(f"Persistent state not found: {output_pickle_path}\nThis should mean you are starting from scratch!")


def save_persistent(output_root):
    pickle_dict = {
        "md5_seen": md5_seen,
        "url_seen": url_seen,
        "num_total": num_total,
        "num_removed_md5": num_removed_md5,
        "num_removed_url": num_removed_url
    }

    output_pickle_path = output_root + "/exact_dedup_checkpoint.pickle"
    assert ROOT_IN not in output_pickle_path
    with open(output_pickle_path, 'wb') as f:
        pickle.dump(pickle_dict, f)

    print(f"Wrote state to persistent storage: {output_pickle_path}")


def main(args):

    global ROOT_IN
    ROOT_IN = args.input_root_dir

    load_persistent(args.output_root_dir)

    for input_path in args.input_files:
        print()
        lang_to_file = {}
        for lang in lang_dirs:
            output_lang_dir = args.output_root_dir + "/" + lang
            # output_path = input_path.replace(args.input_root_dir, args.output_root_dir + "/" + lang)
            output_file_path_lang = input_path.replace(args.input_root_dir, output_lang_dir)
            lang_to_file[lang] = output_file_path_lang

        if check_if_file_done(lang_to_file):
            print(f"Skipping already finished file: {input_path}")
            continue
        # print(f"input_path={input_path}\noutput_path={output_path}")
        print(f"input_path={input_path}")
        read_work_write(input_path, lang_to_file, deduplicate, args.input_root_dir)
        print(f"#total={num_total}, #md5_remove={num_removed_md5}, #url_remove={num_removed_url}")
        print(f"%removed={round(100 * (num_removed_md5 + num_removed_url) / num_total, 2)}")
        save_persistent(args.output_root_dir)

    # save_persistent(args.output_root_dir)


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
    main(args)
