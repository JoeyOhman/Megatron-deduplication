import os
import threading
import queue
from typing import Callable, Dict, Tuple
from pathlib import Path

QUEUE_SIZE = 10000
sentinel = object()


def _read_file(file_name, in_queue):
    with open(file_name, 'r') as f:
        for line in f:
            in_queue.put(line)
    in_queue.put(sentinel)


def _process_line(work_func, in_queue, out_queue):
    for line in iter(in_queue.get, sentinel):
        res = work_func(line)
        out_queue.put(res)

    out_queue.put(sentinel)


def _write_file(file_map, out_queue, root_in, dirty_data_file_path):
    lang_to_file_handle = {}
    dirty_data_file_handle = None
    for line, lang, keep in iter(out_queue.get, sentinel):

        if keep == 0:
            # If the sample should not be kept, put it in dirty directory
            if dirty_data_file_handle is None:
                assert root_in not in dirty_data_file_path
                Path(os.path.dirname(dirty_data_file_path)).mkdir(parents=True, exist_ok=True)
                dirty_data_file_handle = open(dirty_data_file_path, 'w')
            f = dirty_data_file_handle

        else:
            if lang not in lang_to_file_handle:
                # If the sample should be kept, put it in language-specific directory
                output_file_path = file_map[lang]
                assert root_in not in output_file_path
                Path(os.path.dirname(output_file_path)).mkdir(parents=True, exist_ok=True)
                lang_to_file_handle[lang] = open(output_file_path, 'w')
            f = lang_to_file_handle[lang]

        # f = lang_to_file_handle[lang]
        f.write(line + "\n")

    for file_handle in lang_to_file_handle.values():
        if file_handle is not None:  # probably not necessary
            file_handle.close()


def read_work_write(in_file: str, out_files: Dict[str, str], work_func: Callable[[str], Tuple[str, str, int]],
                    root_in: str, dirty_data_root: str) -> None:
    """
    Uses 3 threads including the calling thread, to read a file line by line, do some work on the string, and write it
    to another file.

    :param in_file: file to read lines from
    :param out_files: file map
    :param work_func: function str -> str that will process lines one at a time
    """

    in_queue = queue.Queue(maxsize=QUEUE_SIZE)
    out_queue = queue.Queue(maxsize=QUEUE_SIZE)
    threading.Thread(target=_read_file, args=(in_file, in_queue)).start()
    threading.Thread(target=_process_line, args=(work_func, in_queue, out_queue)).start()
    _write_file(out_files, out_queue, root_in, dirty_data_root)
