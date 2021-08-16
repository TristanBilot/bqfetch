from math import ceil
import numpy as np
import billiard as multiprocessing
from typing import Iterable, List
from joblib import Parallel, delayed

def scope_splitter(target_scope: list, chunk_reference_size) -> List[list]:
    '''
    Split a list into multiple arrays depending on memory size
    '''
    nb_barcodes = len(target_scope)
    nb_chuncks = ceil(nb_barcodes / chunk_reference_size)
    chunks = np.array_split(target_scope, nb_chuncks)
    return list(chunks)

def divide_in_chunks(seq: Iterable, n: int) -> Iterable:
    '''
    Divide an array in `n` arrays of approximately the same length.

    Parameters
    ----------
    seq: Iterable
        The list to divide in chunks
    n: int
        The number of chunks

    Returns
    -------
    out: Iterable
        A list of `n` lists
    '''
    avg = len(seq) / float(n)
    out = []
    last = 0.0
    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg
    return out

# def do_parallel(function, num_cores, partition_list):
#     pool = multiprocessing.Pool(processes=num_cores)
#     return pool.map(function, partition_list)

def do_parallel(function, num_cores, partition_list):
    return Parallel(n_jobs=num_cores)(delayed(function)(item) for item in partition_list)