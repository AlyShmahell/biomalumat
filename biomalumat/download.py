import os
import uuid
import requests
from   tqdm import tqdm 
from   dataclasses import dataclass

@dataclass
class Download:
    path : str = ""
    name : str = ""
    ext  : str = ""
    def __post_init__(self):
        self.full : str = os.path.join(self.path, f"{self.name}.{self.ext}")
    def __str__(self):
        return f"{self.path}, {self.name}, {self.ext}, {self.full}"

class Downloader:
    @classmethod
    def download(self, r, path, name, total_size, block_size):
        with tqdm(
            desc       = f"downloading {name}",
            total      = total_size, 
            unit       = 'iB', 
            unit_scale = True
        ) as t:
            with open(os.path.join(path, name), 'wb') as f:
                for data in r.iter_content(block_size):
                    t.update(len(data))
                    f.write(data)
        if t.n != total_size >= 0:
            raise Exception(f"downloaded {t.n}/{total_size}")
    def __new__(self, url, root:str=None, sub:str=None):
        try:
            r          = requests.get(url, stream=True)
            total_size = int(r.headers.get('content-length', 0))
            if total_size == 0:
                raise Exception("size cannot be 0")
            block_size = 1024**2
            name       = url.split('/')[-1]
            root       = root if root is not None else os.getcwd()
            sub        = sub  if sub  is not None else uuid.uuid4().hex
            path       = os.path.join(root, sub)
            os.makedirs(path, exist_ok=True)
            if not os.path.exists(os.path.join(path, name)) or os.stat(os.path.join(path, name)).st_size != total_size:
                self.download(r, path, name, total_size, block_size)
            name    = name.split('.')
            (
                name, 
                ext
            ) = ".".join(name[:-1]), name[-1]
            return Download(path, name, ext)
        except Exception as e:
            raise Exception(f"download of {url} failed: {e}")