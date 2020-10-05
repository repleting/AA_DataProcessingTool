# Read metadata files (RNH)

from pandas import read_csv, read_excel


def nl32(file_in):

    metadata_in = read_csv(
        file_in,
        index_col=0,
        header=None,
        squeeze=True
    )
    metadata_in["Frequency Weighting"] = metadata_in["Frequency-weight"].replace(' ', '')

    return metadata_in


def nl52(file_in):

    metadata_in = read_csv(
        file_in,
        skiprows=4,
        index_col=0,
        header=None,
        squeeze=True
    )

    return metadata_in


def duo(file_in):

    metadata_in = read_excel(
        file_in,
        nrows=8,
        usecols=[0, 1],
        index_col=0,
        header=None,
        squeeze=True
    )

    metadata_in["Frequency Weighting"] = metadata_in["Weighting"]

    return metadata_in
