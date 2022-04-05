import argparse
import json
import threading
import queue
from typing import Callable

QUEUE_SIZE = 10000
sentinel = object()

UNIQUE_PREFIX = "$€¥"
ID_COUNTER = 0
FILE_NAME = "None"
KEY = "None"


def _read_file(file_name, in_queue):
    with open(file_name, 'r') as f:
        for line in f:
            in_queue.put(line)
    in_queue.put(sentinel)


def _process_line(work_func, in_queue, out_queue):
    for line in iter(in_queue.get, sentinel):
        processed_line = work_func(line)
        out_queue.put(processed_line)

    out_queue.put(sentinel)


def _write_file(file_name, out_queue):
    first_passed = False
    with open(file_name, 'w') as f:
        for line in iter(out_queue.get, sentinel):
            if not first_passed:
                first_passed = True
            else:
                f.write("\n")
            f.write(line)


def read_work_write(in_file: str, out_file: str, work_func: Callable[[str], str]) -> None:
    """
    Uses 3 threads including the calling thread, to read a file line by line, do some work on the string, and write it
    to another file.

    :param in_file: file to read lines from
    :param out_file: file to write processed lines to
    :param work_func: function str -> str that will process lines one at a time
    """

    in_queue = queue.Queue(maxsize=QUEUE_SIZE)
    out_queue = queue.Queue(maxsize=QUEUE_SIZE)
    threading.Thread(target=_read_file, args=(in_file, in_queue)).start()
    threading.Thread(target=_process_line, args=(work_func, in_queue, out_queue)).start()
    _write_file(out_file, out_queue)


# >>> EXAMPLE USAGE <<<
def add_id(json_string):
    json_obj = json.loads(json_string)
    if KEY in json_obj:
        return json_string.strip()

    global ID_COUNTER
    json_obj[KEY] = UNIQUE_PREFIX + "_" + FILE_NAME + "_" + str(ID_COUNTER)
    ID_COUNTER += 1

    return json.dumps(json_obj, ensure_ascii=False)


def remove_id(json_string):
    json_obj = json.loads(json_string)
    if UNIQUE_PREFIX + "_" + FILE_NAME + "_" in json_obj[KEY]:
        del json_obj[KEY]

    return json.dumps(json_obj, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--in_file', type=str, default=None,
                        help='Input .jsonl file')
    parser.add_argument('--out_file', type=str, default=None,
                        help='Output .jsonl file')
    parser.add_argument('--id_key_name', type=str, default=None,
                        help='Key to identifier, e.g. url or doc_id')
    parser.add_argument('--remove', action='store_true',
                        help='Use this to process large number of documents.')
    args = parser.parse_args()

    assert args.in_file != args.out_file

    global KEY, FILE_NAME
    KEY = args.id_key_name
    FILE_NAME = args.in_file.split("/")[-1].split(".")[0]

    work_func = remove_id if args.remove else add_id

    read_work_write(args.in_file, args.out_file, work_func)


if __name__ == '__main__':
    main()
