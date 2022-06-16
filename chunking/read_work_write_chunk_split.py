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


def _write_file(output_file_path, out_queue, root_in, num_chunks):

    chunk_to_file_handle = []
    for i in range(num_chunks):
        output_file_path_chunk = output_file_path.format(idx=i)
        assert root_in not in output_file_path_chunk
        Path(os.path.dirname(output_file_path_chunk)).mkdir(parents=True, exist_ok=True)
        chunk_to_file_handle.append(open(output_file_path_chunk, 'w'))

    chunk_counter = 0
    for line in iter(out_queue.get, sentinel):
        f = chunk_to_file_handle[chunk_counter]

        f.write(line.strip() + "\n")
        chunk_counter = (chunk_counter + 1) % num_chunks

    for file_handle in chunk_to_file_handle:
        file_handle.close()


def read_work_write(in_file: str, out_file: str, work_func: Callable[[str], str],
                    root_in: str, num_chunks: int) -> None:
    """
    Uses 3 threads including the calling thread, to read a file line by line, do some work on the string, and write it
    to another file.

    :param in_file: file to read lines from
    :param out_file: file map
    :param work_func: function str -> str that will process lines one at a time
    """

    in_queue = queue.Queue(maxsize=QUEUE_SIZE)
    out_queue = queue.Queue(maxsize=QUEUE_SIZE)
    threading.Thread(target=_read_file, args=(in_file, in_queue)).start()
    threading.Thread(target=_process_line, args=(work_func, in_queue, out_queue)).start()
    _write_file(out_file, out_queue, root_in, num_chunks)
