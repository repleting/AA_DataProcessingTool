# Use metadata information to find data files

from os import path
from pathlib import Path


def nl52(metadata, metadata_file):

    file_path, file_base = path.split(metadata_file)

    files = []
    for f in Path(file_path + '\\AUTO_LEQ').rglob('*' + metadata.loc['Store Name'][1] + '*.rnd'):
        files.append(f)

    return files


def nl32(metadata, metadata_file):
    # Needs to match metadata to data properly

    file_path, file_base = path.split(metadata_file)

    files = []
    # for f in Path(file_path).rglob('*' + metadata.loc['File Name'][1] + '*.rnd'):   # File Name doesn't look right
    for f in Path(file_path).rglob('*.rnd'):
        files.append(f)

    return files
