import json
import random


def main():
    with open("data_in_not_used/conversations_is.jsonl", 'r') as f:
        json_lines_reddit = f.readlines()
    with open("data_in_not_used/litteraturbanken.jsonl", 'r') as f:
        json_lines_lb = f.readlines()

    print(f"len reddit={len(json_lines_reddit)}")
    print(f"len lb={len(json_lines_lb)}")
    combined = [line.strip() for line in json_lines_reddit + json_lines_lb if len(line.strip()) > 0]
    print(f"len combined={len(combined)}")
    random.shuffle(combined)
    print(f"len combined={len(combined)}")
    num_tot = len(combined)

    num_above = 0
    new_combined = []
    for line in combined:
        if len(line.split(" ")) >= 70:
            num_above += 1
            new_combined.append(line)

    combined = new_combined
    print("Num above 70:", num_above)
    print("Percent above 70:", num_above / num_tot)
    num_tot = len(combined)

    print("100p:", num_tot)
    print("75p:", int(num_tot * 0.75))
    print("50p:", int(num_tot * 0.50))
    print("25p:", int(num_tot * 0.25))
    with open("data_in_not_used/filtered_reddit_is_plus_lb100p.jsonl", 'w') as f:
        for line in combined:
            f.write(line + "\n")
    with open("data_in_not_used/filtered_reddit_is_plus_lb75p.jsonl", 'w') as f:
        for line in combined[:int(num_tot * 0.75)]:
            f.write(line + "\n")
    with open("data_in_not_used/filtered_reddit_is_plus_lb50p.jsonl", 'w') as f:
        for line in combined[:int(num_tot * 0.50)]:
            f.write(line + "\n")
    with open("data_in_not_used/filtered_reddit_is_plus_lb25p.jsonl", 'w') as f:
        for line in combined[:int(num_tot * 0.25)]:
            f.write(line + "\n")
    exit()

    with open("CCMatrix-is-no.jsonl", 'r') as f:
        json_lines = f.readlines()

    # texts = []
    print("num docs:", len(json_lines))

    with open("data_in/CCMatrix-is-no_merged.jsonl", 'w') as f:
        for line in json_lines:
            line_dict = json.loads(line)
            key1, key2 = line_dict.keys()
            text = line_dict[key1] + line_dict[key2]
            text = text.strip()
            text_dict = json.dumps({"text": text}, ensure_ascii=False)
            f.write(text_dict)
            f.write("\n")


if __name__ == '__main__':
    main()
