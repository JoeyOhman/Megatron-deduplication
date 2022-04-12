import json
import random
import numpy as np

# IN_FILE = "/home/joey/reddit_conversations/conversations_lang_split/conversations_da.jsonl"
# IN_FILE = "/home/joey/reddit_conversations/conversations_lang_split/conversations_is.jsonl"
# IN_FILE = "/home/joey/reddit_conversations/conversations_lang_split/conversations_no.jsonl"
# IN_FILE = "/home/joey/reddit_conversations/conversations_lang_split/conversations_sv.jsonl"
# IN_FILE = "/home/joey/reddit_conversations/conversations_lang_split/conversations_nordic.jsonl"
# IN_FILE = "/home/joey/code/ai/opus/data/jsonl/nordic-nordic/CCMatrix-da-sv.jsonl"
# IN_FILE = "/home/joey/code/ai/opus/data/jsonl/nordic-nordic/DGT-da-sv.jsonl"
# IN_FILE = "/home/joey/code/ai/opus/data/jsonl/nordic-nordic/EUbookshop-nb-sv.jsonl"
# IN_FILE = "/home/joey/code/ai/opus/data/jsonl/nordic-nordic/GNOME-nn-sv.jsonl"
# IN_FILE = "/home/joey/code/ai/opus/data/jsonl/nordic-nordic/MultiCCAligned-is-no.jsonl"
# IN_FILE = "/home/joey/code/ai/opus/data/jsonl/nordic-nordic/OpenSubtitles-da-is.jsonl"
# IN_FILE = "/home/joey/code/ai/opus/data/jsonl/nordic-nordic/MultiParaCrawl-da-is.jsonl"
# IN_FILE = "/home/joey/code/ai/opus/data/jsonl/nordic-nordic/WikiMatrix-is-sv.jsonl"
# IN_FILE = "/home/joey/code/ai/opus/data/jsonl/nordic-nordic/wikimedia-da-sv.jsonl"
# IN_FILE = "/home/joey/code/ai/opus/data/jsonl/nordic-nordic/wikimedia-da-sv.jsonl"
IN_FILE = "/home/joey/code/ai/deduplication_repos/Megatron-deduplication/sample_data/opus/templated/ex-en-sv.jsonl"


def main():
    with open(IN_FILE, 'r') as f:
        json_lines = f.readlines()

    # samples = np.random.choice(json_lines, 10, replace=False)

    strings = []
    indices_used = []
    texts_templated = []

    with open("sample_data/opus/templated/pretty_" + IN_FILE.split("/")[-1], 'w') as f:
        for line in json_lines:
            print(line)
            json_dict = json.loads(line)
            text = json_dict["text"]
            f.write("*" * 50 + "\n")
            f.write(text)
            f.write("\n")

    exit()
    for i in range(20):
        idx = random.randint(0, len(json_lines) - 1)
        while idx in indices_used:
            idx = random.randint(0, len(json_lines) - 1)

        # s = random.choice(json_lines)
        indices_used.append(idx)
        s = json_lines[idx]
        json_dict = json.loads(s)
        string = json.dumps(json_dict, ensure_ascii=False, indent=4) + "\n"
        strings.append(string)
        print(json.dumps(json_dict, ensure_ascii=False, indent=4))
        print("*" * 50)

    with open("sample_data/opus/templated/" + IN_FILE.split("/")[-1], 'w') as f:
        for s in strings:
            f.write(s)


if __name__ == '__main__':
    main()
