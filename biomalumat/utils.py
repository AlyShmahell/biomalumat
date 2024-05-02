import os
import functools as fn 
import operator  as op
import numpy     as np

def ncr(n, r):
    r     = min(r, n-r)
    numer = fn.reduce(op.mul, range(n, n-r, -1), 1)
    denom = fn.reduce(op.mul, range(1, r+1), 1)
    return  numer / denom


def sizentropy(sizes):
    v, f = np.unique(sizes, return_counts=True)
    l    = len(sizes)
    p    = f/l
    return np.ceil(sum([(int(os.statvfs('/').f_bsize*1024*8)*p[i])/(v[i]) for i in range(len(v))])).astype(np.int64)

def minsuba(chunk_sizes, t_size):
    v, f = np.unique(chunk_sizes, return_counts=True)
    l    = len(chunk_sizes)
    p    = f/l
    n    = np.ceil(sum([(t_size*p[i])/(v[i]) for i in range(len(v))]))
    return int(n)