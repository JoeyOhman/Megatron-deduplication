

def main():
    file_name = "oscarv310_sv.jsonl"
    base_path = "/home/joey/code/ai/deduplication_repos/Megatron-deduplication/data_out/sv/"
    sub_paths = [
        "chunk_0/oscar/sv/" + file_name,
        "chunk_1/oscar/sv/" + file_name,
        "chunk_2/oscar/sv/" + file_name,
        "chunk_3/oscar/sv/" + file_name,
    ]
    chunk_paths = [base_path + s for s in sub_paths]
    merged_path = "/home/joey/code/ai/deduplication_repos/Megatron-deduplication/data_out_merged/oscar/sv/" + file_name

    with open(merged_path, 'r') as f:
        lines_merged = f.readlines()

    chunk_lines = []
    for chunk_path in chunk_paths:
        with open(chunk_path, 'r') as chunk_f:
            chunk_lines.append(chunk_f.readlines())

    # print(len(chunk_lines[0]))
    # exit()
    concat_lines = chunk_lines[3] + chunk_lines[0] + chunk_lines[1] + chunk_lines[2]
    for m_line, c_line in zip(lines_merged, concat_lines):
        # print("******" * 10)
        print(m_line)
        print(c_line)
        assert m_line == c_line


if __name__ == '__main__':
    main()
