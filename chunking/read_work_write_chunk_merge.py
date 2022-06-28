import os
import multiprocessing
import threading
import queue
from functools import partial
from typing import Callable, List, Tuple
from pathlib import Path

QUEUE_SIZE = 100000
sentinel = object()


def _read_file(file_names, in_queue):
    for file_name in file_names:
        with open(file_name, 'r') as f:
            for line in f:
                in_queue.put(line)

    in_queue.put(sentinel)


# def _process_line_job(line, work_func):
    # return work_func(line)


def _process_line(work_func, in_queue, out_queue, num_workers):

    pool = multiprocessing.Pool(num_workers)

    # job_partial = partial(_process_line_job, work_func=work_func)
    result_iter = pool.imap(work_func, iter(in_queue.get, sentinel), chunksize=64)

    # for line in iter(in_queue.get, sentinel):
    for res, keep in result_iter:
        # res, keep = work_func(line)
        out_queue.put((res, keep))

    pool.close()
    pool.join()

    out_queue.put(sentinel)


def _write_file(output_file_path, out_queue, root_out):
    # assert os.path.dirname(output_file_path) != os.path.dirname(root_in)
    # Path(os.path.dirname(output_file_path)).mkdir(parents=True, exist_ok=True)

    kept_path = output_file_path.replace(root_out, root_out + "/kept")
    removed_path = output_file_path.replace(root_out, root_out + "/removed")

    Path(os.path.dirname(kept_path)).mkdir(parents=True, exist_ok=True)
    Path(os.path.dirname(removed_path)).mkdir(parents=True, exist_ok=True)

    # with open(output_file_path, 'w') as f:
    with open(kept_path, 'w') as f_kept, open(removed_path, 'w') as f_removed:
        for line, keep in iter(out_queue.get, sentinel):
            f = f_kept if keep else f_removed
            f.write(line.strip() + "\n")


def read_work_write(in_files: List[str], out_file: str, work_func: Callable[[str], Tuple[str, str]], root_out: str,
                    num_workers: int) -> None:
    """
    Uses 3 threads including the calling thread, to read a file line by line, do some work on the string, and write it
    to another file.

    :param in_files: files to read lines from
    :param out_file: file to write lines to
    :param work_func: function str -> str that will process lines one at a time
    """

    in_queue = queue.Queue(maxsize=QUEUE_SIZE)
    out_queue = queue.Queue(maxsize=QUEUE_SIZE)
    threading.Thread(target=_read_file, args=(in_files, in_queue)).start()
    threading.Thread(target=_process_line, args=(work_func, in_queue, out_queue, num_workers)).start()
    _write_file(out_file, out_queue, root_out)
