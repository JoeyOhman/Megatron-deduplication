import argparse
import json


def main(in_files_added_ids, in_file_groups, id_key_name):

    json_dicts_added_ids = []
    for in_file in in_files_added_ids:
        with open(in_file, 'r') as f:
            json_lines = f.readlines()
        json_dicts_added_ids += [json.loads(line) for line in json_lines]

    json_dicts_added_ids_dict = {
        json_dict[id_key_name]: json_dict['text'] for json_dict in json_dicts_added_ids
    }
    # print(json_dicts_added_ids_dict)

    with open(in_file_groups, 'r') as f:
        json_lines = f.readlines()

    json_dicts_groups: list[dict] = [json.loads(line) for line in json_lines]
    keys_grouped = [list(group_dict.values())[0] for group_dict in json_dicts_groups]

    print("\n\n### Printing duplicate groups from which only one sample is kept:")
    for i, keys in enumerate(keys_grouped):
        print("\n\n")
        print(f"\tGroup {i}:")
        print("*" * 100)
        print("-" * 90)
        for key in keys:
            print(json_dicts_added_ids_dict[key])
            print("-" * 90)
        print("*" * 100)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--in_files_added_ids', nargs='*', default=None,
                        help='Input .jsonl file with added unique ids')
    parser.add_argument('--in_file_groups', type=str, default=None,
                        help='Input .jsonl with duplicate groups to be removed')
    parser.add_argument('--id_key_name', type=str, default=None,
                        help='Key to identifier, e.g. url or doc_id')
    args = parser.parse_args()

    main(args.in_files_added_ids, args.in_file_groups, args.id_key_name)
