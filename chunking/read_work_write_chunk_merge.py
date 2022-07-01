import os
import multiprocessing
import threading
import queue
from functools import partial
from typing import Callable, List, Tuple, Any, Optional
from pathlib import Path

QUEUE_SIZE = 100000
sentinel = object()


len_utf8bytes_kept = 0
len_char_kept = 0
len_utf8bytes_removed = 0
len_char_removed = 0

doc_counter = 0


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
    for res, keep, metrics in result_iter:
        # res, keep = work_func(line)
        out_queue.put((res, keep, metrics))

    pool.close()
    pool.join()

    out_queue.put(sentinel)


def _write_file(output_file_path, out_queue, root_out):
    # assert os.path.dirname(output_file_path) != os.path.dirname(root_in)
    # Path(os.path.dirname(output_file_path)).mkdir(parents=True, exist_ok=True)

    kept_path = output_file_path.replace(root_out, root_out + "/kept")
    removed_path = output_file_path.replace(root_out, root_out + "/removed")
    error_path = output_file_path.replace(root_out, root_out + "/error")

    global len_utf8bytes_kept, len_char_kept, len_utf8bytes_removed, len_char_removed, len_char_removed, doc_counter

    f_kept, f_removed, f_error = None, None, None
    # with open(output_file_path, 'w') as f:
    # with open(kept_path, 'w') as f_kept, open(removed_path, 'w') as f_removed, open(error_path, 'w') as f_error:
    for line, keep, metrics in iter(out_queue.get, sentinel):
        # ERROR PATH
        if keep is None:
            if f_error is None:
                Path(os.path.dirname(error_path)).mkdir(parents=True, exist_ok=True)
                f_error = open(error_path, 'w')
            f = f_error
        else:
            len_utf8bytes, len_char = metrics

            if keep:  # KEPT PATH
                if f_kept is None:
                    Path(os.path.dirname(kept_path)).mkdir(parents=True, exist_ok=True)
                    f_kept = open(kept_path, 'w')
                f = f_kept
                len_utf8bytes_kept += len_utf8bytes
                len_char_kept += len_char
            else:
                if f_removed is None:  # REMOVED PATH
                    Path(os.path.dirname(removed_path)).mkdir(parents=True, exist_ok=True)
                    f_removed = open(removed_path, 'w')
                f = f_removed
                len_utf8bytes_removed += len_utf8bytes
                len_char_removed += len_char

            # f = f_kept if keep else f_removed

        f.write(line.strip() + "\n")

        doc_counter += 1
        if doc_counter % 10000000 == 0:
            print(f"\n#docs={int(doc_counter / 1000000)}M")
            print(f"Kept: {int(len_utf8bytes_kept / 1000000000)}GB, {int(len_char_kept / 1000000000)}B chars")
            print(f"Removed: {int(len_utf8bytes_removed / 1000000000)}GB, {int(len_char_removed / 1000000000)}B chars")

    print(f"\n#docs={int(doc_counter / 1000000)}M")
    print(f"Kept: {int(len_utf8bytes_kept / 1000000000)}GB, {int(len_char_kept / 1000000000)}B chars")
    print(f"Removed: {int(len_utf8bytes_removed / 1000000000)}GB, {int(len_char_removed / 1000000000)}B chars")

    if f_kept is not None:
        f_kept.close()
    if f_removed is not None:
        f_removed.close()
    if f_error is not None:
        f_error.close()


def read_work_write(in_files: List[str], out_file: str, work_func: Callable[[str], Tuple[str, Optional[int], Any]], root_out: str,
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
