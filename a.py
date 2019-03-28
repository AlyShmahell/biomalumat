"""
import pathos.multiprocessing as pmp
import multiprocessing
import os

class A(object):
    def __init__(self):
        self.map = multiprocessing.Manager().dict()
        self.reduce = multiprocessing.Manager().dict()
    def __mapper(self, x, y):
        print(os.getpid())
        if x in self.map:
            self.map[x] += y
            self.reduce[x] -= y
        else:
            self.map[x] = 0
            self.reduce[x] = 0
        return x
    def __call__(self):
        data = [1,1,1,2,3,4,2,4,1]
        with pmp.Pool(pmp.cpu_count()) as pool:
            pool.map(lambda x: self.__mapper(x, 10), data)
        print(self.map, self.reduce)

if __name__ == '__main__':
    a = A()
    a()
"""
from pathos.multiprocessing import Pool
import multiprocessing
from tqdm import tqdm
import time

import os
import multiprocess
import pathos

class NonDaemonicProcess(multiprocess.Process):
    """
    NoDaemonProcess Class.
        sets daemon to false.
    """
    def _get_daemon(self):
        return False
    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon)


class NestedPool(pathos.multiprocessing.Pool):
    """
    NestedPool Class.
        enables nested process pool.
    """
    Process = NonDaemonicProcess

class A(object):
    def __init__(self):
        self.bigmap = multiprocessing.Manager().dict()
    def _mapper(self, my_number, a):
        self.bigmap[my_number] = my_number * my_number * a
    def _foo(self, my_number, a):
        self.bigmap[my_number * my_number * a] = my_number * my_number * a
        with NestedPool(2) as pool:
            pool.imap(lambda b: self._mapper(b, 5), range(30))
    def _bar(self):
        with NestedPool(2) as pool:
            for _ in tqdm(pool.imap(lambda b: self._foo(b, 5), range(30))):
                pass
    def __call__(self):
        self._bar()
    def __str__(self):
        return f"{self.bigmap}"

if __name__ == '__main__':
    a = A()
    a()
    print(a)
                