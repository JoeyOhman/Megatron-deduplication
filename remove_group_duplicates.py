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


import json
import time
import sys


if __name__ == '__main__':

    id_filename = sys.argv[1]
    data_filename = sys.argv[2]
    output_filename = sys.argv[3]
    identifier_key = sys.argv[4]

    # Set of ids that should be removed
    ids_to_remove = set()
    with open(id_filename, 'r') as f:
        for line in f:
            myjson = json.loads(line)
            for key in myjson:
                this_ids = myjson[key]
                for i in range(1, len(this_ids)):
                    ids_to_remove.add(this_ids[i])
    print('will be removing {} ids'.format(len(ids_to_remove)), flush=True)

    kept_docs = 0
    kept_chars = 0
    removed_docs = 0
    removed_chars = 0
    start_time = time.time()
    with open(output_filename, 'wb') as fout:
        with open(data_filename, 'r') as fin:
            for line in fin:
                try:
                    myjson = json.loads(line)
                    id = myjson[identifier_key]
                    if id in ids_to_remove:
                        removed_docs += 1
                        removed_chars += len(myjson['text'])
                        if "keep" in myjson and "filters" in myjson:
                            myjson["keep"] = 0
                            myjson["filters"].append("fuzzy_dedup")
                        else:
                            continue
                    else:
                        kept_docs += 1
                        kept_chars += len(myjson['text'])
                    myjson = json.dumps(myjson, ensure_ascii=False)
                    fout.write(myjson.encode('utf-8'))
                    fout.write('\n'.encode('utf-8'))
                    # kept_docs += 1
                    if kept_docs % 100000 == 0:
                        print(' [PROCESSED] time (s): {:.2f} | kept: {} '
                              '| removed: {} (chars: {})'.format(
                                  time.time() - start_time,
                                  kept_docs, removed_docs, removed_chars))
                except Exception as e:
                    print('[SKIPPING]', line, e)

    print(' [PROCESSED] time (s): {:.2f} | kept: {} '
          '| removed: {} (chars: {})'.format(
              time.time() - start_time,
              kept_docs, removed_docs, removed_chars))

    print(f"Done with {output_filename} :-)")
    if kept_docs + removed_docs == 0:
        doc_fraction, char_fraction = 0, 0
    else:
        doc_fraction = round(100 * removed_docs / (kept_docs + removed_docs), 2)
        char_fraction = round(100 * removed_chars / (kept_chars + removed_chars), 2)
    print(f"Removed {doc_fraction}% of documents.")
    print(f"Removed {char_fraction}% of characters.")
