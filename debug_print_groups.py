import argparse
import json
import numpy as np
from nltk.translate.bleu_score import sentence_bleu
from nltk.translate.bleu_score import SmoothingFunction
from multiprocessing import Pool
import spacy

nlp = spacy.load('en_core_web_sm')


def calcBleu(reference, candidate, weights):
    return sentence_bleu(reference, candidate, weights=weights, smoothing_function=SmoothingFunction().method1)


def calculateSelfBLEU(texts, ngram=5):
    if len(texts) == 1:
        return 0

    spacyTexts = list(nlp.pipe(texts))
    textsSplits = np.array([[token.text for token in t] for t in spacyTexts], dtype=object)

    # textsSplits = np.array([[token.text for token in nlp(t)] for t in texts], dtype=object)
    arange = np.arange(len(textsSplits))
    weights = tuple((1. / ngram for _ in range(ngram)))

    # pool = Pool(2)
    bleus = list()
    for idx, candidate in enumerate(textsSplits):
        reference = textsSplits[arange != idx].tolist()
        # bleu = sentence_bleu(reference, candidate, weights=weights, smoothing_function=SmoothingFunction().method1)
        # bleus.append(pool.apply_async(calcBleu, args=(reference, candidate, weights)))
        bleus.append(calcBleu(reference, candidate, weights))

    # for idx, b in enumerate(bleus):
    #     bleus[idx] = b.get()

    # pool.close()
    # pool.join()

    return np.mean(bleus)


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

    # Calculate self-bleus in parallel
    print("Calculating Self-BLEUs")
    pool = Pool(8)
    grouped_tuples = []
    sbs = []
    for i, keys in enumerate(keys_grouped):
        texts = [json_dicts_added_ids_dict[key] for key in keys]
        # sb = calculateSelfBLEU(texts)
        sbs.append(pool.apply_async(calculateSelfBLEU, args=(texts,)))
        grouped_tuples.append([i, texts])

    for idx, sb in enumerate(sbs):
        sbs[idx] = sb.get()

    pool.close()
    pool.join()
    print("Self-BLEUs done!")

    for idx, tup in enumerate(grouped_tuples):
        tup.append(sbs[idx])

    grouped_tuples = sorted(grouped_tuples, key=lambda x: x[-1])

    print("\n\n### Printing duplicate groups from which only one sample is kept:")
    # for i, keys in enumerate(keys_grouped):
    for tup in grouped_tuples:
        i, texts, sb = tup
        print("\n\n")
        print(f"\tGroup {i}, group size={len(texts)}, sb={sb}:")
        print("*" * 100)
        print("-" * 90)
        for t in texts:
            # print(json_dicts_added_ids_dict[key])
            print(t)
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
