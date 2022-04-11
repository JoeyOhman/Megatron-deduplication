import json


def main():
    with open("CCMatrix-is-no.jsonl", 'r') as f:
        json_lines = f.readlines()

    # texts = []
    print("num docs:", len(json_lines))

    with open("data_in_not_used/CCMatrix-is-no_merged.jsonl", 'w') as f:
        for line in json_lines[:50000]:
            line_dict = json.loads(line)
            key1, key2 = line_dict.keys()
            text = line_dict[key1] + line_dict[key2]
            text = text.strip()
            text_dict = json.dumps({"text": text}, ensure_ascii=False)
            f.write(text_dict)
            f.write("\n")


if __name__ == '__main__':
    main()
