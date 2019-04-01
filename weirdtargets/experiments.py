import pandas as pd
import numpy as np
import dask.dataframe as dd
import collections
import itertools
import time
from tqdm import tqdm
from numba import njit
import multiprocessing
import pathos.multiprocessing as pmp
from dask.threaded import get as ddscheduler
from multiprocessing import Manager, Lock

class A(object):
    def __init__(self):
        self.namespace = Manager().Namespace()
        self.namespace.counter = collections.Counter()
        self.data = [np.array(range(8)) for _ in range(10)]
    def __call__(self):
        data = [np.array(range(8)) for _ in range(10)]
        def __parallelCounterUpdate(x):
            print("in parallel")
            with Lock():
                self.namespace.counter += collections.Counter([f"{sorted(combination)}" for combination in itertools.combinations(x, 2)])
        with pmp.Pool(pmp.cpu_count(), maxtasksperchild=2) as pool:
            pool.imap(__parallelCounterUpdate, self.data)
            pool.close()
            pool.join()
            pool.terminate()
        print(self.namespace.counter)

if __name__ == '__main__':
    a = A()
    start = time.time()
    a()
    print(time.time() - start)


"""
pd.DataFrame.from_records({"a": [0, 1, 2, 1, 2], "b": [10, 20, 30, 40, 50]}).to_hdf(
    './tmp/experiments.h5',
    key="experiments",
    mode="a",
    append="true",
    min_itemsize={"a": 50}
)
ddf = dd.read_hdf('./tmp/experiments.h5',
            key='experiments').compute(scheduler=ddscheduler)
print(ddf.head())

def mode(x):
    v, c = np.unique(x, return_counts = True)
    return v[c.argmax()]
bags = ddf.groupby(["a", "b"]).b.apply(mode)
index = ddf.a[~ddf.a.duplicated()]
print("~~~~~~~~~~~Content:~~~~~~~~~~~~")
for i in index:
    print (i, bags[i].values)
print("~~~~~~~~~~~Counter:~~~~~~~~~~~~")
c = collections.Counter(['eggs', 'ham', 'eggs'])
print(c['ham'])
print(c['figs'])
print(c)
s = time.time()
c[f"{np.array(range(2))}"] = c.get('figs', 0) + 1
print(f"time: {time.time()-s}")
print("~~~~~~~~~~~combins:~~~~~~~~~~~~")
def combinations(a):
    i, j = np.triu_indices(len(a), 0)
    b = [[i[x], j[x]] for x in range(len(i)) if i[x] != j[x]]
    i = [x[0] for x in b]
    j = [x[1] for x in b]
    return np.unique(np.stack([a[i], a[j]]).T, axis=0).tolist()
a = np.array(['a', 'b', 'c', 'd'])
b = np.array(range(400, 1200))
s = time.time()
d = collections.Counter([f"{sorted(z)}" for z in combinations(a)])
d += collections.Counter([f"{sorted(z)}" for z in combinations(a)])
print(f"counter combin - time: {time.time()-s}, result: {len(d)}")
s = time.time()
d = collections.Counter([f"{sorted(z)}" for
                         z in itertools.combinations(a, 2)])
d += collections.Counter([f"{sorted(z)}" for z in itertools.combinations(a, 2)])
print(f"counter combin - time: {time.time()-s}, result: {len(d)}")
s = time.time()
print(f"numpy   combin - time: {time.time()-s}, result: {len(combinations(a))}")
s = time.time()
print(f"regular combin - time: {time.time()-s}, result: {len(list(itertools.combinations(a, 2)))}")

"""

