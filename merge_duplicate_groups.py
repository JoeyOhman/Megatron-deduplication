import argparse
import json
from typing import List, Set

import tqdm


def read_and_merge_groups(input_files: List[str]) -> List[Set[str]]:
    """
    Reads all files and groups within them, and merges groups together when they contain an identical document

    :param input_files: input files with groups of document ids
    :return: list of merged duplicate groups
    """

    # Maps document id, to index in existing_groups list (constant lookup)
    doc_to_idx = {}
    # Contains groups (Python set) of document ids
    existing_groups = []
    # all_docs_found = set()
    # Go through files (36)
    for in_file in sorted(input_files):
        print(in_file)
        with open(in_file, 'r') as f:
            lines = f.readlines()
        # Go through lines in current file (a lot ~1M)
        for line in tqdm.tqdm(lines):
            if line.strip() == "":
                continue
            json_obj = json.loads(line)
            # _, new_group_ids = json_obj.items()[0]
            # print(json_obj)
            # print(json_obj[next(iter(json_obj))])
            new_group_ids = json_obj[next(iter(json_obj))]

            # List of groups that should be merged, due to the new current group
            group_indices_found = []

            # Go through current group of document ids ()
            for doc_id in new_group_ids:
                group_idx = doc_to_idx.get(doc_id, None)
                if group_idx is not None:
                    group_indices_found.append(group_idx)

            # groups_found = set(groups_found)
            new_group_ids = set(new_group_ids)
            # all_docs_found |= new_group_ids

            if len(group_indices_found) == 0:
                # No groups found, no merging, just add it to the list of existing groups!
                existing_groups.append(new_group_ids)
                for id in new_group_ids:
                    doc_to_idx[id] = len(existing_groups) - 1
            else:

                # Only keep unique group indices
                group_indices_found = set(group_indices_found)
                # Sort in reverse -> list
                group_indices_found = sorted(group_indices_found, reverse=True)

                # lowest index will be merge_group, rest will be merged into it
                merged_first_group_idx, group_indices_found = group_indices_found[-1], group_indices_found[:-1]
                # Select one group at random that all groups will get merged to, NOT RANDOM ANYMORE
                # merged_first_group = existing_groups[group_indices_found.pop()]
                merged_first_group = existing_groups[merged_first_group_idx]

                # Merge groups into the selected group, and remove the others from the list of existing groups
                groups_found_without_target = [existing_groups[i] for i in group_indices_found] + [new_group_ids]
                groups_found = groups_found_without_target + [merged_first_group]

                merged_first_group = set().union(*groups_found)
                # for group_index_found in group_indices_found:
                #     merged_first_group |= existing_groups[group_index_found]

                for group_index_found in group_indices_found:
                    existing_groups[group_index_found] = set()

                # Finally, merge in the new group as well!
                # merged_first_group |= new_group_ids
                # for doc_id in merged_first_group:
                for doc_id_set in groups_found_without_target:
                    for doc_id in doc_id_set:
                        doc_to_idx[doc_id] = merged_first_group_idx

                existing_groups[merged_first_group_idx] = merged_first_group

    # print("Num unique docs found:", len(all_docs_found))
    return existing_groups


def main(args) -> None:
    """
    Main driver of this file, merge groups of duplicates from several files and writes one file with merged groups

    :param args: argparse object
    """
    groups = read_and_merge_groups(args.inputs)

    # docs_kept = set()
    # num_docs_kept = 0
    with open(args.output, 'w') as f:
        for idx, group in enumerate(groups):
            if len(group) == 0:
                continue
            # num_docs_kept += len(group)
            # docs_kept |= group
            json_line = json.dumps({idx: list(group)}, ensure_ascii=False)
            f.write(json_line + "\n")

    # print("Unique docs in num_docs_kept:", num_docs_kept)
    # print("Unique docs in num_docs_kept (dbl check):", len(docs_kept))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputs', nargs='*', default=None,
                        help='List of the input files, e.g. --inputs similar_documents_chunks_0_1.jsonl '
                             'similar_documents_chunks_0_2.jsonl')
    parser.add_argument('--output', type=str, default=None,
                        help='Output file name that contains the merged groups, e.g. similar_documents_merged.jsonl')
    args = parser.parse_args()
    main(args)
