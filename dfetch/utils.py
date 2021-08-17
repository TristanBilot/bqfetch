from math import ceil
import multiprocessing

import numpy as np
import billiard as billiard_multiprocessing
from typing import Iterable, List
from joblib import Parallel, delayed

def scope_splitter(target_scope: list, chunk_reference_size) -> List[list]:
    '''
    Split a list into multiple arrays depending on memory size.
    '''
    nb_barcodes = len(target_scope)
    nb_chuncks = ceil(nb_barcodes / chunk_reference_size)
    chunks = np.array_split(target_scope, nb_chuncks)
    return list(chunks)

def divide_in_chunks(seq: Iterable, n: int) -> Iterable:
    '''
    Divide an array in `n` arrays of approximately the same length.
    '''
    avg = len(seq) / float(n)
    out = []
    last = 0.0
    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg
    return out

def do_parallel_billiard(function, num_cores, partition_list):
    '''
    Run `function` in parallel using `num_cores` processes with each
    call using as parameters the tuple at the ith index of `partition_list`.

    Billiard library is an old fork of Python multiprocessoing lib which
    allow forking processes from daemon process.
    '''
    pool = billiard_multiprocessing.Pool(processes=num_cores)
    return pool.map(function, partition_list)

def do_parallel_joblib(function, num_cores, partition_list):
    '''
    Run `function` in parallel using `num_cores` processes with each
    call using as parameters the tuple at the ith index of `partition_list`.

    Warning: using joblib do not allow to create child processes from
    a daemon process (because joblib use multiprocessing as backend).
    So in many cases (ex: running this function from Airflow),
    using this function will result in computing  multiprocessing with 
    n_jobs=1 so no parallel processing at all.
    Prefer using `do_parallel()` function using billiard (old fork of
    multiprocessing whiwh allow process spawn from daemon).
    '''
    return Parallel(n_jobs=num_cores)(delayed(function)(item) for item in partition_list)

def do_parallel_multiprocessing(function, num_cores, partition_list):
    '''
    Run `function` in parallel using `num_cores` processes with each
    call using as parameters the tuple at the ith index of `partition_list`.
    '''
    pool = multiprocessing.Pool(processes=num_cores)
    return pool.map(function, partition_list)

def log(*args):
    print()
    for arg in args:
        print(f'>>> {arg}')

def ft(size_in_GB: int) -> str:
    '''
    Formats gigabytes like 2.3892... = 2.38GB
    '''
    return f'{round(size_in_GB, 2)}GB'