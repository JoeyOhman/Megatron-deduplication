# coding=utf-8
# Copyright (c) 2020, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
from functools import partial
import itertools
import json
from lsh import cache, minhash
import multiprocessing
import numpy as np
import time
import pickle
import sys
import os
import psutil
# from numba import jit

max_ram_usage = 0


def get_current_ram_usage():
    global max_ram_usage
    current_process = psutil.Process(os.getpid())
    current_ram_usage = current_process.memory_full_info().pss
    for child in current_process.children(recursive=True):
        current_ram_usage += child.memory_full_info().pss
    max_ram_usage = max(current_ram_usage, max_ram_usage)
    print(f"CURRENT RAM USAGE: {current_ram_usage // 1000000}, MAX RAM USAGE: {max_ram_usage // 1000000}MB")


# This function is adapted from:
#   https://github.com/mattilyra/LSH/blob/master/examples/Introduction.ipynb
# @jit(nopython=True)
def shingles(text: str, char_ngram: int):
    return set(text[head:head + char_ngram] for head in range(0, len(text) - char_ngram))
    # my_set = set()
    # for head in range(0, len(text) - char_ngram):
    #     my_set.add(text[head:head + char_ngram])
    # return my_set


# This function is adapted from:
#  https://github.com/mattilyra/LSH/blob/master/examples/Introduction.ipynb
def jaccard(set_a, set_b, args):
    if len(set_a) < 1 or len(set_b) < 1:
        return 0.0

    intersection = set_a & set_b
    union = set_a | set_b

    if args.jaccard == 'min':
        return len(intersection) / min(len(set_a), len(set_b))
    elif args.jaccard == 'max':
        return len(intersection) / max(len(set_a), len(set_b))
    else:
        return len(intersection) / len(union)


def compute_fingerprint(line, key):
    try:
        myjson = json.loads(line)
        if "keep" in myjson and myjson["keep"] == 0:
            return None, None, None, False
        doc_id = myjson[key]
        text = myjson['text']
        fingerprint = hasher.fingerprint(text)
    except Exception as e:
        print('Error:', e)
        return None, None, None, False

    return doc_id, text, fingerprint, True


def doc_id_pairs_to_remove(args, bucket_ids, id_doc):
    remove_ids_list = []
    deduped_local, counter_local = 0, 0
    iteration = 0
    while len(bucket_ids) > 1:
        if args.heuristic_iter != -1 and iteration == args.heuristic_iter:
            break

        # print(f"iteration={iteration}, len(bucket_urls)={len(bucket_urls)}")
        items = list(bucket_ids)
        remove_ids = []
        main_id = items[np.random.randint(0, len(items))]
        main_shingles = shingles(id_doc[main_id], args.char_n_gram)

        for i in range(0, len(items)):
            counter_local += 1
            other_id = items[i]
            if other_id == main_id:
                continue
            other_shingles = shingles(id_doc[other_id], args.char_n_gram)
            try:
                jaccard_sim = jaccard(main_shingles, other_shingles, args)
            except Exception as e:
                print('Error:', e)
                jaccard_sim = 0.0
            # print(i, other_url, jaccard_sim)
            if jaccard_sim > args.jaccard_threshold:
                remove_ids.append({other_id: jaccard_sim})
                deduped_local += 1
                bucket_ids.remove(other_id)

        bucket_ids.remove(main_id)
        if len(remove_ids) > 0:
            remove_ids_list.append({main_id: remove_ids})
        iteration += 1
    return remove_ids_list, deduped_local, counter_local


def write_remove_ids_list(remove_ids_list, f_out):
    if len(remove_ids_list) > 0:
        for each_id_remove in remove_ids_list:
            myjson = json.dumps(each_id_remove, ensure_ascii=False)
            f_out.write(myjson.encode('utf-8'))
            f_out.write('\n'.encode('utf-8'))


def compute_jaccard(each_bin, num_bins, start_time_local):

    remove_ids_list = []
    deduped_local, counter_local, bucket_local = 0, 0, 0

    for bucket_id in each_bin:
        bucket_local += 1
        if os.getpid() % num_bins == 0 and bucket_local % 100000 == 0:
            print("Counter {}, progress {:.2f} time {:.2f}".format(
                bucket_local, float(bucket_local)/float(len(each_bin)), time.time() - start_time_local), flush=True)

        # print("len(each_bin[bucket_id]):", len(each_bin[bucket_id]))
        if len(each_bin[bucket_id]) <= 1:
            continue

        bucket_ids = each_bin[bucket_id].copy()
        remove_ids_list_sub, deduped_local_sub, counter_local_sub = doc_id_pairs_to_remove(args, bucket_ids, id_doc)

        deduped_local += deduped_local_sub
        counter_local += counter_local_sub
        if len(remove_ids_list_sub) > 0:
            remove_ids_list.extend(remove_ids_list_sub)

    return remove_ids_list, deduped_local, counter_local


def find_pair_ids_parallel(args, lshcache, id_doc):
    start_time = time.time()
    f_out = open(args.output, 'wb')
    deduped, counter = 0, 0

    # compute jaccards of buckets in bin in parallel (parallelism
    # limited to # of bins)
    num_bins = len(lshcache.bins)
    pool = multiprocessing.Pool(min(num_bins, args.max_workers_jaccard))
    compute_jaccard_partial = partial(compute_jaccard, num_bins=num_bins, start_time_local=start_time)
    # don't need to pass args and url_doc as they are already shared
    compute_jaccard_iter = pool.imap(compute_jaccard_partial, lshcache.bins)

    print("multiprocessing init took {:.2f}".format(time.time() - start_time), flush=True)

    # for i in range(15):
    #     time.sleep(2)
    #     get_current_ram_usage()

    for remove_ids_list, deduped_local, counter_local in compute_jaccard_iter:
        # get_current_ram_usage()
        deduped += deduped_local
        counter += counter_local
        write_remove_ids_list(remove_ids_list, f_out)
        print(' [write]> processed {} documents in {:.2f} '
              'seconds and deduped {} documents ...'.format(counter, time.time() - start_time, deduped), flush=True)

    pool.close()
    pool.join()
    f_out.close()

    print(' Taken time for jaccard similariries {:.2f} seconds'.format(time.time() - start_time), flush=True)


def find_pair_ids_sequential(args, lshcache, id_doc):
    start_time = time.time()
    f_out = open(args.output, 'wb')
    deduped, counter = 0, 0
    for b in lshcache.bins:
        for bucket_id in b:
            if len(b[bucket_id]) <= 1:
                continue

            bucket_ids = b[bucket_id].copy()
            remove_ids_list_sub, deduped_local_sub, counter_local_sub = doc_id_pairs_to_remove(args, bucket_ids, id_doc)

            deduped += deduped_local_sub
            counter += counter_local_sub
            write_remove_ids_list(remove_ids_list_sub, f_out)
            if counter % 100000 == 0:
                print(' [write]> processed {} documents in {:.2f} seconds and deduped {} documents ...'.format(
                    counter, time.time() - start_time, deduped), flush=True)
    f_out.close()
    print(' [write]> processed {} documents in {:.2f} seconds and deduped {} documents ...'.format(
        counter, time.time() - start_time, deduped), flush=True)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--seed', type=int, default=1234,
                       help='Random seed used for python, numpy')
    parser.add_argument('--inputs', nargs='*', default=None,
                        help='List of the input files, e.g. --inputs cc.json news.json')
    parser.add_argument('--doc_id_key', type=str, default="md5",
                        help='The key to the doc_id, e.g. \"md5\"')
    parser.add_argument('--load_fingerprints', nargs='*', default=None,
                       help='Load fingerprints from a list of pickle files, e.g. cc.pkl news.pkl')
    parser.add_argument('--save_fingerprints', type=str, default=None,
                       help='Save the fingerprints of the inputs.')
    parser.add_argument('--output', type=str, default=None,
                       help='Output file name that consists of all ids with matching similarities')
    parser.add_argument('--jaccard', type=str, default='union',
                        choices=['union', 'min', 'max'], help='Jaccard similarity computation')
    parser.add_argument('--heuristic_iter', type=int, default=1,
                       help='Number of iterations to run the heuristics: use -1 for exact')
    parser.add_argument('--num_bands', type=int, default=10,
                       help='Number of bands to use in cache')
    parser.add_argument('--num_seeds', type=int, default=100,
                       help='Number of seeds to use for minhash. Note that this value should be divisible by num-bands')
    parser.add_argument('--char_n_gram', type=int, default=5,
                        help='Number of chars to create char-n-gram shingles from.')
    parser.add_argument('--max_workers_fingerprints', type=int, default=None,
                        help='Maximum number of CPU workers to use for fingerprints (not so RAM intensive and '
                             'scales well).')
    parser.add_argument('--max_workers_jaccard', type=int, default=None,
                        help='Maximum number of CPU workers to use for jaccard (scales well, but scales RAM linearly!)')
    parser.add_argument('--jaccard_threshold', type=float, default=0.5,
                        help='The jaccard similarity threshold, above which documents are removed.')
    args = parser.parse_args()

    assert args.max_workers_fingerprints is not None
    assert args.max_workers_jaccard is not None
    assert args.inputs is not None

    # set seed and get an array of seeds of 100 integers
    np.random.seed(args.seed)
    seeds = np.random.randint(0, 1e6, size=args.num_seeds)

    # initialize minhash and lsh cache
    hasher = minhash.MinHasher(seeds=seeds, char_ngram=args.char_n_gram, hashbytes=4)
    lshcache = cache.Cache(num_bands=args.num_bands, hasher=hasher)

    id_doc = {}

    """
    # load fingerprints from pickle file if needed
    if args.load_fingerprints is not None:
        for count_fp, fp_file_name in enumerate(args.load_fingerprints):
            print("Loading fingerprints from pickle file {}".format(
                fp_file_name), flush=True)
            fp = open(fp_file_name, "rb")
            if count_fp == 0:
                # assign directory for the first pkl
                lshcache = pickle.load(fp)
                url_doc = pickle.load(fp)
            else:
                # append these to lshcache and url_doc
                local_lshcache = pickle.load(fp)
                local_url_doc = pickle.load(fp)
                for url in local_lshcache.fingerprints.keys():
                    url_doc[url] = local_url_doc[url]
                    lshcache.add_fingerprint(local_lshcache.fingerprints[url], url)
            fp.close()
    """

    counter = 0
    start_time = time.time()

    # if args.inputs is not None:
    print("Computing fingerprints", flush=True)
    # assert len(args.inputs) % 2 == 0
    # for input_file, key in zip(args.inputs[::2], args.inputs[1::2]):
    key = args.doc_id_key
    for input_file in args.inputs:
        print(' processing {} with key {}'.format(input_file, key), flush=True)

        # Compute fingerprints in parallel
        pool = multiprocessing.Pool(args.max_workers_fingerprints)
        fin = open(input_file, 'r', encoding='utf-8')

        compute_fingerprint_partial = partial(compute_fingerprint, key=key)
        compute_fingerprint_iter = pool.imap(compute_fingerprint_partial, fin, 512)

        # Traverse all the texts and add fingerprints
        for id, text, fingerprint, flag in compute_fingerprint_iter:
            if id is None:
                continue

            counter += 1
            # if counter % 5000 == 0:
            #     get_current_ram_usage()

            if flag:
                id_doc[id] = text
                lshcache.add_fingerprint(fingerprint, id)
            if counter % 100000 == 0:
                print(' [read]> processed {} documents in {:.2f} seconds ...'.format(
                    counter, time.time() - start_time), flush=True)

        fin.close()
        pool.close()
        pool.join()

    # Save the fingerprints if needed
    """
    if args.save_fingerprints is not None:
        print("Saving fingerprints to pickle file {}".format(
            args.save_fingerprints), flush=True)
        with open(args.save_fingerprints, 'wb') as f_save:
            pickle.dump(lshcache, f_save)
            pickle.dump(url_doc, f_save)
    """

    # Compute Jaccard index of the input texts and write to file if needed
    if args.output is not None:
        print("Computing Jaccard similarity", flush=True)
        if args.max_workers_jaccard > 1:
            find_pair_ids_parallel(args, lshcache, id_doc)
        else:
            find_pair_ids_sequential(args, lshcache, id_doc)
