import argparse
import json

# A B C
# B C D -> A B C D
# E F G H -> A B C D, E F G H
# I J B E -> first group, but should really link both groups together??


def read_groups(input_files):
    existing_groups = []
    for in_file in input_files:
        with open(in_file, 'r') as f:
            for line in f:
                if line.strip() == "":
                    continue
                json_obj = json.loads(line)
                _, group_ids = json_obj.items()[0]

                groups_found = []
                for group_idx, existing_group in enumerate(existing_groups):
                    for doc_id in group_ids:
                        if doc_id in existing_group:
                            groups_found.append(group_idx)

                # TODO: Merge groups
                group_ids = set(group_ids)


def main(args):
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputs', nargs='*', default=None,
                        help='List of the input files, e.g. --inputs similar_documents_chunks_0_1.jsonl '
                             'similar_documents_chunks_0_2.jsonl')
    args = parser.parse_args()
    main(args)
