import json
import random


def split_text_into_random_parts(text):
    min_chars = 500
    max_chars = 3000

    split_texts = []
    start_idx = 0
    while start_idx < len(text):
        end_idx = start_idx + random.randint(min_chars, max_chars)
        split_texts.append(text[start_idx: end_idx])
        start_idx = end_idx

    return split_texts


def create_dups(book_dicts):
    dups = []
    num_dups_exact = 0
    num_dups_fuzzy = 0

    for book in book_dicts:
        if random.random() < 0.5:
            continue

        title = book["title"]
        text = book["text"]

        fuzzy = random.random() < 0.5
        if fuzzy:
            num_dups_fuzzy += 1
            fuzzy_words = text.split(" ")
            for i, w in enumerate(fuzzy_words):
                if random.random() < 0.02:  # corrupt 2% of words
                    fuzzy_words[i] = random.choice(fuzzy_words)
            text = " ".join(fuzzy_words)
        else:
            num_dups_exact += 1

        dups.append({"title": title, "text": text})

    print("Num dups exact:", num_dups_exact)
    print("Num dups fuzzy:", num_dups_fuzzy)
    to_ret = book_dicts + dups
    random.shuffle(to_ret)
    return to_ret


def main():
    with open("data_in_not_used/litteraturbanken.jsonl", 'r') as f:
        json_lines = f.readlines()

    json_dicts = [json.loads(line) for line in json_lines[50:]]

    book_dicts = []
    titles = []
    for idx in range(0, len(json_dicts), 10):
        json_dict = json_dicts[idx]
        if "Inträdestal" in json_dict["title"] and "13 maj" not in json_dict["title"]:
            continue
        titles.append(json_dict["title"])
        book_dicts.append(json_dict)

    for title in sorted(titles):
        print(title)

    # Split books
    split_books = []
    for book in book_dicts:
        title = book["title"]
        text = book["text"]
        split_texts = split_text_into_random_parts(text)

        split_dicts = [{"title": title, "text": st} for st in split_texts]
        split_books += split_dicts

    book_dicts = split_books

    # Create Duplicates
    print("Len before dups:", len(book_dicts))
    book_dicts = create_dups(book_dicts)
    print("Len after dups:", len(book_dicts))

    with open("data_in/LB_split.jsonl", 'w') as f:

        for book in book_dicts:
            f.write(json.dumps(book, ensure_ascii=False) + "\n")


if __name__ == '__main__':
    random.seed(42)
    main()
