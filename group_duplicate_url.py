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

    print('grouping duplicate ids ...')

    input = sys.argv[1]
    output = sys.argv[2]
    if len(sys.argv) > 3:
        jaccard_similarity_threshold = float(sys.argv[3])
    else:
        jaccard_similarity_threshold = 0.5

    id_to_index = {}
    index_to_ids = []
    counter = 0
    start_time = time.time()
    with open(input, 'r') as f:
        for line in f:
            counter += 1
            myjson = json.loads(line)
            ids = []
            for main_id in myjson.keys():
                ids.append(main_id)
                for value in myjson[main_id]:
                    for other_id, js in value.items():
                        if js >= jaccard_similarity_threshold:
                            ids.append(other_id)
            current_index = -1
            other_indices = set()
            for id in ids:
                if id in id_to_index:
                    if current_index == -1:
                        current_index = id_to_index[id]
                    elif current_index != id_to_index[id]:
                        other_indices.add(id_to_index[id])
            if current_index == -1:
                current_index = len(index_to_ids)
                index_to_ids.append(set())
            for id in ids:
                id_to_index[id] = current_index
                index_to_ids[current_index].add(id)
            for index in other_indices:
                for id in index_to_ids[index]:
                    index_to_ids[current_index].add(id)
                    id_to_index[id] = current_index
                index_to_ids[index] = None

            if counter % 100000 == 0:
                print(' > processed {} lines in {} seconds ...'.format(
                    counter, time.time() - start_time))

    total_remove = 0
    total_remain = 0
    for ids in index_to_ids:
        if ids is not None:
            if len(ids) > 1:
                total_remove += (len(ids) - 1)
                total_remain += 1
    print('out of {} ids, only {} are unique and {} should be removed'.format(
        total_remove+total_remain, total_remain, total_remove))

    with open(output, 'wb') as f:
        for i, ids in enumerate(index_to_ids):
            if ids is not None:
                if len(ids) > 1:
                    myjson = json.dumps({str(i): list(ids)},
                                        ensure_ascii=False)
                    f.write(myjson.encode('utf-8'))
                    f.write('\n'.encode('utf-8'))
