import pandas as pd 
import dask.dataframe as dd
from dask.threaded import get as ddscheduler

def a(x:"value") -> "returns errors":
    return x

pd.DataFrame.from_records({"a": [0]}).to_hdf(
    './tmp/experiments.h5',
    key="experiments",
    mode="a",
    append="true",
    min_itemsize={"a": 50}
)
ddf:"the ddf" = dd.read_hdf('./tmp/experiments.h5',
            key='experiments').compute(scheduler=ddscheduler)
print(ddf.head())
pd.DataFrame.from_records({"a": [1,2,3]}).to_hdf(
    './tmp/experiments.h5',
    key="experiments",
    mode="a",
    append="true",
    min_itemsize={"a": 50}
)
print(a(1))
print(ddf.head())