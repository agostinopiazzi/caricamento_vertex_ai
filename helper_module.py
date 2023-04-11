import sys
from typing import Optional

def get_input(sc, input_name:str, read_kwargs:Optional[dict] = None):
    list = sys.argv[1:-1]
    list = list.sort(key=lambda x: x.split('/')[-1])
    list = [x.split('/')[-1] for x in list]
    index = list.index(input_name)
    if read_kwargs:
        sdf = sc.read_parquet(sys.argv[index], **read_kwargs)
    else:
        sdf = sc.read_parquet(sys.argv[index])
    return sdf


def set_output(sdf, output_name:str, write_kwargs:Optional[dict] = None):
    if write_kwargs:
        sdf.write_parquet(sys.argv[-1], **write_kwargs)
    else:
        sdf.write_parquet(sys.argv[-1])