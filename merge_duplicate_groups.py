import argparse
import json

# A B C
# B C D -> A B C D
# E F G H -> A B C D, E F G H
# I J B E -> first group, but should really link both groups together??


def read_and_merge_groups(input_files):
    doc_to_idx = {}
    existing_groups = []
    # all_docs_found = set()
    for in_file in input_files:
        with open(in_file, 'r') as f:
            for line in f:
                if line.strip() == "":
                    continue
                json_obj = json.loads(line)
                # _, new_group_ids = json_obj.items()[0]
                # print(json_obj)
                # print(json_obj[next(iter(json_obj))])
                new_group_ids = json_obj[next(iter(json_obj))]

                group_indices_found = []

                for doc_id in new_group_ids:
                    group_idx = doc_to_idx.get(doc_id, None)
                    if group_idx is not None:
                        group_indices_found.append(group_idx)

                # for group_idx, existing_group in enumerate(existing_groups):
                #     for doc_id in new_group_ids:
                #         if doc_id in existing_group:
                #             group_indices_found.append(group_idx)

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
                    # print(group_indices_found)
                    # lowest index will be merge_group, rest will be merged into it
                    merged_first_group_idx, group_indices_found = group_indices_found[-1], group_indices_found[:-1]
                    # Select one group at random that all groups will get merged to, NOT RANDOM ANYMORE
                    # merged_first_group = existing_groups[group_indices_found.pop()]
                    # print(merged_first_group_idx, len(existing_groups))
                    merged_first_group = existing_groups[merged_first_group_idx]

                    # Merge groups into the selected group, and remove the others from the list of existing groups
                    # for group_index_found in sorted(group_indices_found, reverse=True):
                    for group_index_found in group_indices_found:
                        merged_first_group |= existing_groups[group_index_found]
                        existing_groups[group_index_found] = set()
                        # del existing_groups[group_index_found]

                    # Finally, merge in the new group as well!
                    merged_first_group |= new_group_ids
                    for doc_id in merged_first_group:
                        doc_to_idx[doc_id] = merged_first_group_idx

    # print("Num unique docs found:", len(all_docs_found))
    return existing_groups


def main(args):
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
