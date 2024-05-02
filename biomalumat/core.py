import os
import sys
import json
import gzip
import joblib
import itertools
from   tqdm                import tqdm
from   collections         import defaultdict
from   biomalumat.utils    import ncr
from   biomalumat.download import Downloader

class DiseaseTargetCycleEnumeration:
    def map(self):
        combinations = itertools.combinations(self.dtcc.keys(), 2)
        nck          = int(ncr(len(self.dtcc), 2))
        base         = int(nck/os.cpu_count())
        prev         = 0
        with tqdm(desc=f"Mapping") as tkdm:
            for curr in range(base, nck, base):
                tkdm.update(base)
                yield itertools.islice(
                    combinations,
                    prev,
                    curr,
                    1
                ) 
                prev  = curr
            if curr < nck:
                tkdm.update(nck-curr)
                yield itertools.islice(
                    combinations,
                    curr,
                    nck,
                    1
                )
    def worker(self, idx, chunk):
        local = 0
        with tqdm(
            desc=f'Reducing #{idx}', 
            leave=True, 
            file=sys.stdout, 
            position=idx
        ) as tkdm:
            for lhs, rhs in chunk:
                #print(self.dtcc[lhs], self.dtcc[rhs], self.dtcc[lhs] & self.dtcc[rhs])
                local += len(self.dtcc[lhs] & self.dtcc[rhs]) >= 2
                tkdm.update(1)
        return local
    def reduce(self):
        with joblib.parallel_config(backend="multiprocessing"):
            print(
                sum(
                    joblib.Parallel(verbose=0, n_jobs=os.cpu_count())(
                        joblib.delayed(self.worker)(idx, chunk) for idx, chunk in enumerate(self.map())
                    )
                )
            )
    def search(self, parsed, keys):
        if len(keys)>1:
            return self.search(parsed.get(keys[0], {}), keys[1:])
        return parsed.get(keys[0], None)
    def __init__(self, url, lhs, rhs, root, sub):
        self.dtcc = defaultdict(set)
        download  = Downloader(url, root, sub)
        with gzip.GzipFile(download.full) as f:
            with tqdm(desc=f"processing {download.full}") as tkdm:
                for line in f:
                    parsed = json.loads(line)
                    east   = self.search(parsed, lhs)
                    west   = self.search(parsed, rhs)
                    self.dtcc[east] |= {west}
                    tkdm.update(1)
        self.reduce()