from __future__ import division
from __future__ import print_function
from __future__ import generator_stop
from __future__ import unicode_literals
from __future__ import absolute_import
import argparse, re, math, json, itertools
import os, sys, psutil, time, datetime, inspect
import dask
import ujson
import dask.array as da
import dask.dataframe as dd
import pandas as pd, numpy as np
import pathos.multiprocessing as pmp
from tqdm import tqdm
from gzip import GzipFile
from toolz import partition_all
from dask.diagnostics import visualize
from dask.diagnostics import ProgressBar
from dask.diagnostics import ResourceProfiler
from dask.threaded import get as ddscheduler
from opentargets import OpenTargetsClient


class WeirdTargetsException(Exception):
    """
    Exception Class
    """
    __module__ = Exception.__module__

    def __init__(self, error):
        try:
            line = sys.exc_info()[-1].tb_lineno
        except AttributeError:
            line = inspect.currentframe().f_back.f_lineno
        self.args = f"{type(self).__name__} (line {line}): {error}",
        sys.exit(self)


class WeirdTargetsPrinter(object):
    """
    Printer Class
    """

    def _pretty(self, x):
        """
        An assistant helper function:
            assists printing functions,
            by removing white spaces after newlines.
        """
        return re.sub(r"\n\s+", "\n", x)

    def _oneliner(self, x):
        """
        An assistant helper function:
            assists printing functions,
            by removing newlines and subsequent whitespaces.
        """
        return re.sub(r"\n\s*", " ", x)


class WeirdTargetsArgParse(WeirdTargetsPrinter):
    """
    Base Parser Class

    """

    def __getitem__(self, key):
        """
        A main function:
            makes the class subscriptable
            the only supported method to access parsed arguments.
        """
        return getattr(self.args, key)


class SmallTargetsArgParse(WeirdTargetsArgParse):
    """
    Derived Parser Class
        defines parser functionality specific to 'problem A'.
    """

    def __id_regex(self, id):
        """
        An assistant helper function:
            assists in filtering the <id> argument.
        """
        regex = re.compile(r'^EFO_|^ENSG')
        if not regex.match(id):
            raise WeirdTargetsException(self._oneliner("""--id should
                                                          start with: 
                                                          <EFO_> or 
                                                          <ENSG>"""))
        return id

    def __check_args_compatibility(self, type, id):
        """
        An assistant helper function:
            assists in checking compatibility of <id> and <type>.
        """
        if not((re.match(r'EFO_.+\sdisease', f"{id} {type}"))
                or (re.match(r'ENSG.+\starget', f"{id} {type}"))):
            raise WeirdTargetsException(self._oneliner(f"""type: {type} 
                                                           and id: {id} 
                                                           are incompatible."""))

    def __init__(self):
        """
        A main function:
            Init Function
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("--type",
                            type=str,
                            help=self._oneliner("""specify 
                                                   a type to 
                                                   look for, 
                                                   a target or 
                                                   a disease."""),
                            required=True,
                            choices=['disease', 'target'])
        parser.add_argument("--id",
                            type=self.__id_regex,
                            help="specify an id to look for.",
                            required=True)
        self.args = parser.parse_args()
        self.__check_args_compatibility(self.args.type,
                                        self.args.id)


class BigTargetsArgParse(WeirdTargetsArgParse):
    """
    Derived Parser Class
        defines parser functionality specific to 'problem B'.
    """

    def __init__(self):
        """
        A main function:
            Init Function
        """
        parser = argparse.ArgumentParser()
        parser.add_argument("--filename",
                            type=str,
                            help=self._oneliner("""specify a
                                                     file that 
                                                     holds a 
                                                     collection 
                                                     of json 
                                                     objects."""),
                            required=True)
        self.args = parser.parse_args()


class WeirdTargets(WeirdTargetsPrinter):
    """
    Base Class
        used to define common main functionality.
    """

    def __getPIDs(self, number):
        """
        An assistant helper function:
            assists in testing PID count.
        """
        return f"{os.getpid()}"

    def testParallelism(self):
        """
        A helper function:
            tests PID count.
        """
        with pmp.Pool(pmp.cpu_count()) as pool:
            PIDS = pool.map(self.__getPIDs, range(pmp.cpu_count()+1))
            print(f"Number of Cores Utilized: {np.unique(PIDS).size}")


class SmallTargets(WeirdTargets):
    """
    Derived Class
        used to define main functionality specific to "problem A".
    """

    def __init__(self, type, id):
        """
        A main function:
            Init Function
        """
        self.type = type
        self.id = id
        self.inputs = None
        self.outputs = []
        self.otc = OpenTargetsClient()
        self.elapsed_time = None
        super(SmallTargets, self).__init__()

    def __getOveralls(self, entry):
        """
        An assistant helper function:
            assists in getting 'overall' values in parallel.
        """
        return entry['association_score']['overall']

    def __squareOveralls(self, overall):
        """
        An assistant helper function:
            assists in squaring 'overall' values in parallel.
        """
        return overall**2

    def __call__(self):
        """
        A main function:
            makes the class callable.
            executes the primary functionality: fetch -> process.
        """
        if self.type == "disease":
            try:
                self.inputs = self.otc.get_associations_for_disease(self.id)
            except:
                raise WeirdTargetsException("Incorrect ID")
        if self.type == "target":
            try:
                self.inputs = self.otc.get_associations_for_target(self.id)
            except:
                raise WeirdTargetsException("Incorrect ID")
        if not self.inputs:
            raise WeirdTargetsException(self._oneliner("""The query did not
                                                          return any usefull
                                                          information."""))
        self.elapsed_time = time.time()
        with pmp.Pool(pmp.cpu_count()) as pool:
            overalls = pool.map(self.__getOveralls,    self.inputs)
            squared_overalls = pool.map(self.__squareOveralls, overalls)
            minimum = min(overalls)
            maximum = max(overalls)
            average = sum(overalls)/len(self.inputs)
            standard_deviation = np.sqrt(
                sum(squared_overalls)/len(self.inputs) - average**2)
            self.outputs = [maximum, minimum, average, standard_deviation]
        self.elapsed_time = time.time() - self.elapsed_time

    def __str__(self):
        """
        A main function:
            makes the class printable.
            to be executed after calling the object.
        """
        if not self.outputs:
            raise WeirdTargetsException(self._oneliner("""you need to call 
                                                          the SmallTargets 
                                                          object first."""))
        return self._pretty(f"""Number of Entries:  {len(self.inputs)}\n
                                Elapsed Time:       {self.elapsed_time} sec\n
                                Maximum:            {self.outputs[0]}\n
                                Minumum:            {self.outputs[1]}\n
                                Average:            {self.outputs[2]}\n
                                Standard Deviation: {self.outputs[3]}""")


class BigTargets(WeirdTargets):
    """
    Derived Class
        used to define main functionality specific to "problem B".
    """

    def __init__(self, filename):
        """
        A main function:
            Init Function
        """
        self.filename = filename
        self.outputs = []
        self.associations_set = {}
        self.target_target_count = 0

    def __jsonMapper(self, pyObject):
        """
        An assistant helper function:
            assists in loading a json object.
            is mapped using __pdDataframeMapper.
        """
        parsed = ujson.loads(pyObject)
        obj = {"target": parsed["target"]["id"],
               "disease": parsed["disease"]["id"],
               "score": parsed["scores"]["association_score"]}
        return obj

    def __pdDataframeMapper(self, batch):
        """
        An assistant helper function:
            assists in loading a json batch.
            maps json objects using __jsonMapper.
            is mapped using __H5DiskPersistor.
        """
        posts = map(self.__jsonMapper, batch)
        df = pd.DataFrame.from_records(posts, columns=["target",
                                                       "disease",
                                                       "score"])
        return df

    def __peek(self, iterable):
        """
        An assistant helper function:
            assists in iterating a map.
        """
        try:
            first = next(iterable)
        except StopIteration:
            return None, None
        return first, itertools.chain([first], iterable)

    def __H5DiskPersistor(self):
        """
        An assistant helper function:
            assists in loading the main json collection.
            maps json batches batches using __pdDataframeMapper.
        """
        start_time = time.time()
        with GzipFile(self.filename) as f:
            batches = partition_all(math.floor(
                psutil.virtual_memory()[1]/(1024**3))*20000, f)
            while True:
                df, frames = self.__peek(
                    map(self.__pdDataframeMapper, batches))
                if frames == None:
                    break
                df.to_hdf('data.h5',
                          key='df',
                          mode='a',
                          format='table')
        print(self._oneliner(f"""H4 file has been created in 
                                 {datetime.timedelta(seconds=(
                                 time.time() - start_time))}"""))

    def __DaskDataframeLoader(self):
        progressbar = ProgressBar()
        progressbar.register()
        with ResourceProfiler(dt=0.25) as resource_profile:
            ddf = dd.read_hdf('data.h5', key='df').compute(
                scheduler=ddscheduler)
            print(f"""Number of entries: {ddf["score"].count()}""")
            print(f"""First few entries:\n{ddf.head()}""")
            print(f"""Median of scores: {ddf["score"].quantile(.5)}""")
            with tqdm(iterable=range(1, ddf.target.count() + 1),
                      desc='bagging', file=sys.stdout) as tqdm_object:
                for disease, target in zip(ddf.disease, ddf.target):
                    tqdm_object.update(1)
                    if target not in self.associations_set:
                        self.associations_set[target] = [disease]
                    else:
                        self.associations_set[target].append(disease)
            with tqdm(desc='set operations') as tqdm_object:
                for associations_1, associations_2 in itertools.combinations(
                        self.associations_set.values(),
                        2):
                    tqdm_object.update(1)
                    if len(set(associations_1) & set(associations_2)) >= 2:
                        self.target_target_count += 1
            print(self.target_target_count)
        resource_profile.visualize()

    def __call__(self):
        """
        A main function:
            makes the class callable.
            executes the primary functionality: fetch -> process.
        """
        if not (os.path.exists("data.h5")):
            self.__H5DiskPersistor()
        self.__DaskDataframeLoader()
