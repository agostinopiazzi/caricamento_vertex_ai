import sys
from typing import Optional


def get_input(sc, input_name: str, read_kwargs:Optional[dict] = None):

    input_list = sys.argv[1:-1]
    input_list_names = [x.split('/')[-1] for x in input_list]

    index = input_list_names.index(input_name)
    if read_kwargs:
        sdf = sc.read_parquet(sys.argv[index], **read_kwargs)
    else:
        sdf = sc.read_parquet(sys.argv[index])
    return sdf


def set_output(sdf, write_kwargs: Optional[dict] = None):
    if write_kwargs:
        sdf.write_parquet(sys.argv[-1], **write_kwargs)
    else:
        sdf.write_parquet(sys.argv[-1])