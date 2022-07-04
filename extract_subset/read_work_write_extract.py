import random
import threading
import queue
from typing import List
import tqdm
import json

QUEUE_SIZE = 100000
sentinel = object()


def _read_file(file_names, in_queue, root_in):
    for file_name in tqdm.tqdm(file_names):
        with open(file_name, 'r') as f:
            file_name_no_root = file_name.replace(root_in + "/", "")
            for line in f:
                # Only keep 0.2% of samples => ~2.6GB
                if random.random() < 0.002:
                    obj = json.loads(line)
                    obj["dataset"] = file_name_no_root
                    in_queue.put(json.dumps(obj, ensure_ascii=False))
    in_queue.put(sentinel)


def _write_file(output_file_path, in_queue):

    lines = []
    for line in iter(in_queue.get, sentinel):
        lines.append(line)

    random.shuffle(lines)

    with open(output_file_path, 'w') as f:
        for line in lines:
            f.write(line.strip() + "\n")


def read_work_write(in_files: List[str], out_file: str, root_in: str) -> None:
    """
    Uses 3 threads including the calling thread, to read a file line by line, do some work on the string, and write it
    to another file.

    :param in_files: file to read lines from
    :param out_file: file map
    :param root_in: root in
    """

    in_queue = queue.Queue(maxsize=QUEUE_SIZE)
    threading.Thread(target=_read_file, args=(in_files, in_queue, root_in)).start()
    _write_file(out_file, in_queue)
