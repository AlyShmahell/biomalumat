import numpy as np
import collections
import itertools
import time
import multiprocessing
import pathos.multiprocessing as pmp
from dask.threaded import get as ddscheduler
import collections
from multiprocessing.managers import MakeProxyType, SyncManager, BaseManager, DictProxy

CollectionsCounterBaseProxy = MakeProxyType("CollectionsCounterBaseProxy", 
                            [
                                '__add__', '__and__', '__class__', '__contains__', 
                                '__delattr__', '__delitem__', '__dict__', '__dir__', 
                                '__doc__', '__eq__', '__format__', '__ge__', 
                                 '__getitem__', '__gt__', 
                                '__hash__', '__iadd__', '__iand__', 
                                '__ior__', '__isub__', 
                                '__iter__', '__le__', '__len__', '__lt__', 
                                '__missing__', '__module__', '__ne__', '__neg__', 
                                '__or__', '__pos__', '__reduce__', '__call__',
                                '__reduce_ex__', '__repr__', 
                                '__setitem__', '__sizeof__', '__str__', '__sub__', 
                                '__subclasshook__', '__weakref__', '_keep_positive', 
                                'clear', 'copy', 'elements', 'fromkeys', 'get', 
                                'items', 'keys', 'most_common', 'pop', 'popitem', 
                                'setdefault', 'subtract', 'update', 'values'
                                ]
                        )

class CollectionsCounterProxy(CollectionsCounterBaseProxy):
    def __iadd__(self, value):
        self._callmethod('__iadd__', (value,))
        return self



counter = collections.Counter()
manager = None
def Counter(value=None):
    global counter
    counter += collections.Counter(value)
    return counter
class CounterManager(BaseManager):
    pass
def initManager():
    global manager
    CounterManager.register('Counter', Counter, CollectionsCounterProxy)
    manager = CounterManager()
    manager.start()

class A(object):
    def __init__(self):
        self.data = [np.array(range(400)) for _ in range(100)]
        global manager
        self.counter = manager.Counter()
    def parallelFunction(self, x):
        self.counter += manager.Counter(list(itertools.combinations(x,2)))
    def __call__(self):
        start = time.time()
        with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
            print("hey")
            pool.imap(self.parallelFunction, self.data)
            pool.close()
            pool.join()
        print("yo")
        print(len(self.counter), self.counter[(0,1)])
        print(f"time: {time.time() - start}")

class B(object):
    def __init__(self):
        self.data = [np.array(range(400)) for _ in range(100)]
        self.counter = collections.Counter()        
    def __call__(self):
        start = time.time()
        print("hey")
        for x in self.data:
            self.counter += collections.Counter(list(itertools.combinations(x,2)))
        print("yo")
        print(len(self.counter), self.counter[(0,1)])
        print(f"time: {time.time() - start}")

if __name__ == '__main__':
    initManager()
    a = A()
    a()
    b = B()
    b()


"""
from collections import defaultdict

from multiprocessing.managers import MakeProxyType, SyncManager

DefaultDictProxy = MakeProxyType("DefaultDictProxy", [
    '__contains__', '__delitem__', '__getitem__', '__len__',
    '__setitem__', 'clear', 'copy', 'default_factory', 'fromkeys',
    'get', 'items', 'keys', 'pop', 'popitem', 'setdefault',
    'update', 'values'])

SyncManager.register("defaultdict", defaultdict, DefaultDictProxy)
# Can also create your own Manager here, just using built in for simplicity

if __name__ == '__main__':
    with SyncManager() as sm:
        dd = sm.defaultdict(list)
        print(dd['a'])
"""

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

