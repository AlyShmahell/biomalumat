from __future__ import division
from __future__ import print_function
from __future__ import generator_stop
from __future__ import unicode_literals
from __future__ import absolute_import
import argparse, re, math, json, itertools
import os, sys, psutil, time, datetime, inspect, multiprocess, multiprocessing
import ujson, pathos
import dask.dataframe as dd
import pandas as pd, numpy as np
import pathos.multiprocessing as pmp
from tqdm import tqdm
from gzip import GzipFile
from toolz import partition_all
from dask.threaded import get as ddscheduler
from opentargets import OpenTargetsClient

__copyright__ = "Copyrights © 2019 Aly shmahell."
__credits__   = ["Aly Shmahell"]
__version__   = "0.1.1"
__maintainer__= "Aly Shmahell"
__email__     = ["aly.shmahell@gmail.com"]
__status__    = "Alpha"


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
    def __init__(self):
        """
        Interface Initializer
            used to instantiate common parameters.
        """
        self.args = None
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
        super(SmallTargetsArgParse, self).__init__()
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
        super(BigTargetsArgParse, self).__init__()
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
        parser.add_argument("--features_dump_path",
                            type=str,
                            help=self._oneliner("""specify a
                                                   folder that 
                                                   will hold
                                                   lazily extracted
                                                   features."""),
                            required=True)
        self.args = parser.parse_args()


class WeirdTargets(WeirdTargetsPrinter):
    """
    Base Class
        used to define common main functionality.
    """
    def __init__(self):
        """
        Interface Initializer
            used to instantiate common parameters.
        """
        self.empty_string = ""
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
            self.outputs = {
                "Maximum"           : maximum,
                "Minimum"           : minimum,
                "Average"           : average,
                "Standard Deviation": standard_deviation
            }
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
                                Elapsed Time      :       {self.elapsed_time} sec\n
                                Maximum           :       {self.outputs['Maximum']}\n
                                Minumum           :       {self.outputs['Minimum']}\n
                                Average           :       {self.outputs['Average']}\n
                                Standard Deviation:       {self.outputs['Standard Deviation']}""")


class BigTargets(WeirdTargets):
    """
    Derived Class
        used to define main functionality specific to "problem B".
    """
    def __init__(self, filename, features_dump_path):
        """
        A main function:
            Init Function
        """
        super(BigTargets, self).__init__()
        self.filename = filename
        self.features_dump_path = features_dump_path
        self.ddf = None
        self.map = multiprocessing.Manager().dict()
        self.reduce = multiprocessing.Manager().dict()
        self.target_target_count = 0
        self.outputs = None
    def __jsonMapper(self, pyObject, tqdmObject):
        """
        An assistant helper function:
            assists in loading a json object.
            is mapped using __pdDataframeMapper.
        """
        tqdmObject.update(1)
        parsed = ujson.loads(pyObject)
        obj = {"target": parsed["target"]["id"],
               "disease": parsed["disease"]["id"],
               "score": float(parsed["scores"]["association_score"])}
        return obj
    def __pdDataframeMapper(self, batch, tqdmObject):
        """
        An assistant helper function:
            assists in loading a json batch.
            maps json objects using __jsonMapper.
            is mapped using __H5DiskPersistor.
        """
        parsedJSON = map(lambda b: self.__jsonMapper(b, tqdmObject), batch)
        df = pd.DataFrame.from_records(parsedJSON, columns=["target",
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
            dumps extracted features to H5 file.
        """
        with tqdm(desc=f"Feature Extraction{self.empty_string:>18}") as tqdmObject:
            with GzipFile(self.filename) as f:
                batches = partition_all(math.floor(psutil.virtual_memory()[1]/(1024**3))*20000, f)
                while True:
                    df, frames = self.__peek(map(lambda b: self.__pdDataframeMapper(b, tqdmObject), batches))
                    if frames == None:
                        break
                    df.to_hdf(os.path.join(self.features_dump_path,'bigtargets.h5'),
                              key='df',
                              mode='a',
                              format='table',
                              append=True)
    def __reduceMapper(self, old, disease):
        word = f"{sorted([old, disease])}"
        if word not in self.reduce:
            self.reduce[word] = 1
        else:
            self.reduce[word] += 1
    def __MapReduce(self):
        with pmp.Pool(pmp.cpu_count()) as pool:
            with tqdm(desc="") as tqdmObject:
                for disease, target in zip(self.ddf.disease, self.ddf.target):
                    tqdmObject.update(1)
                    if target not in self.map:
                            self.map[target] = [disease]
                    else:
                        pool.imap(lambda old: self.__reduceMapper(old, disease), self.map[target])
                        self.map[target].append(disease)
        with tqdm(desc=f"Reducing{self.empty_string:>28}") as tqdmObject:
                self.target_target_count = sum([x for x in self.reduce.values() if x >= 2])

    def __call__(self):
        """
        A main function:
            makes the class callable.
            calls __setBagger then __setOperator,
            to calculate Target-Target Pairs Which,
            Share More than 2 Diseases (Bipartite Cycles).
        """
        if not (os.path.exists(os.path.join(self.features_dump_path,'bigtargets.h5'))):
            self.__H5DiskPersistor()
        self.ddf     = dd.read_hdf(os.path.join(self.features_dump_path,'bigtargets.h5'),
                                   key='df').compute(scheduler=ddscheduler)
        self.outputs = {"Number of Entries": self.ddf["score"].count(),
                        "Median of Scores" : self.ddf["score"].quantile(.5)}
        self.__MapReduce()
        self.outputs["Target-Target"] = self.target_target_count
    def __str__(self):
        """
        A main function:
            makes the class printable.
            to be executed after calling the object.
        """
        if not self.outputs:
            raise WeirdTargetsException(self._oneliner("""you need to call 
                                                          the BigTargets 
                                                          object first."""))
        return self._pretty(f"""Number of Entries: {self.outputs['Number of Entries']}\n
                                Median of Scores : {self.outputs['Median of Scores']}\n
                                Target-Target    : {self.outputs['Target-Target']}""")