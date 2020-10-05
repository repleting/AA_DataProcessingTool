# Read data files and convert to standard format DataFrames

from pandas import read_csv, read_excel, DataFrame, Series, Index, MultiIndex, \
    to_datetime, to_timedelta, to_numeric, concat
import read_metadata
import find_data
from datetime import datetime


def read(file_type, files, user_metadata, **columns):

    flag_metadata = False

    # Read metadata
    if file_type == 'nl32_metadata':
        flag_metadata = True
        metadata = read_metadata.nl32(files[0])
        files = find_data.nl32(metadata, files[0])

    if file_type == 'nl52_metadata':
        flag_metadata = True
        metadata = read_metadata.nl52(files[0])
        files = find_data.nl52(metadata, files[0])

    if file_type.startswith('duo'):
        flag_metadata = True
        metadata = read_metadata.duo(files[0])
        files = files[0]

    # Reset file type now data files have been identified
    file_type = file_type.replace('meta', '')

    if not flag_metadata:

        metadata_idx = ['Frequency Weighting']
        for i in range(len(user_metadata)-1):
            metadata_idx.append('Percentile ' + str(i+1))

        metadata = Series(
            data=user_metadata,
            index=Index(metadata_idx, dtype='object', name=0)
        )

    # Read data, concatenating if more than one file is found
    if file_type == 'nl32_data':

        if len(files) == 1:
            data = nl32(files[0], metadata, flag_metadata)
        else:
            for f, file in enumerate(files):
                data_tmp = nl32(file, metadata, flag_metadata)
                if f == 0:
                    data = data_tmp
                else:
                    data = concat([data, data_tmp])

    if file_type == 'nl52_data':

        metrics = [
            'Leq',
            'LE',
            'Lmax',
            'Lmin',
            'LN1',
            'LN2',
            'LN3',
            'LN4',
            'LN5',
            'Over',
            'Under'
        ]

        if len(files) == 1:
            data = nl52(files[0], metadata, metrics)
        else:
            for f, file in enumerate(files):
                data_tmp = nl52(file, metadata, metrics)
                if f == 0:
                    data = data_tmp
                else:
                    data = concat([data, data_tmp])

    if file_type == 'duo_data':
        data = duo(files, metadata, spectral=False)

    if file_type == 'duo_octave_data':
        data = duo(files, metadata, spectral=True)

    if file_type == 'custom_csv':
        data = custom_csv(files[0], columns['columns'])

    if file_type == 'custom_excel':
        data = custom_excel(files[0], columns['columns'])

    # Insert column of sequential integers
    if 'Address' not in data.columns:
        data['Address'] = range(len(data))

    if 'Duration' not in data.columns:
        data['Duration'] = data.index
        data['Duration'] = to_timedelta(data['Duration'].shift(-1) - data.index)

    return data, metadata


def nl32(file_in, metadata, flag_metadata):

    data = read_csv(file_in, index_col='Time', parse_dates=True)
    data['Measurment Time'] = to_timedelta(data['Measurment Time'])
    data = data.rename(columns={'Measurment Time': 'Duration'})

    if flag_metadata:
        f_weight = metadata['Frequency-weight'].replace(' ', '')
    else:
        f_weight = metadata['Frequency Weighting']

    rename = {}
    for c in data.filter(regex='^L' + f_weight).columns:
        rename[c] = c + '_Main'
    data.rename(columns=rename, inplace=True)

    return data     # , f_weight


def nl52(file_in, metadata, metrics):

    idx_in = read_csv(file_in, usecols=range(2), skiprows=1, parse_dates=True, names=['filter', 'value'])

    if idx_in.iloc[3, 0] == 'Frequency Weighting':
        attended = True
        skiprows = 6
        metrics.append('Pause')
    else:
        attended = False
        skiprows = 4

    # TODO: return attended flag
    # TODO: print duration after read if attended

    data_in = read_csv(file_in, skiprows=skiprows, parse_dates=True)
    # Previously had usecols=range(14) for data_in. Removed for flexibility - will this cause problems?

    # Read measurement times and durations
    data = DataFrame()
    data['Time'] = to_datetime(idx_in[idx_in['filter'] == 'Start Time'].reset_index(drop=True).iloc[:, -1])
    data['Duration'] = to_timedelta(idx_in[idx_in['filter'] == 'Measurement Time'].reset_index(drop=True).iloc[:, -1])
    data['Address'] = idx_in[idx_in['filter'] == 'Address'].reset_index(drop=True).iloc[:, -1].astype(int)

    # Read measurement values
    for metric in metrics:

        if metric in ['Over', 'Under', 'Pause']:

            data_metric = data_in[['Unnamed: 0', 'Main']][data_in['Unnamed: 0'] == metric]. \
                reset_index(drop=True). \
                add_prefix(metric + '_')

        else:

            data_metric = data_in[data_in['Unnamed: 0'] == metric].\
                reset_index(drop=True).\
                add_prefix(metric + '_')

        data_metric.drop(columns=data_metric.filter(regex='Unnamed').columns, inplace=True)

        if metric + '_Sub' in data_metric.columns:
            data_metric.drop(columns=[metric + '_Sub'], inplace=True)

        if metric in ['Over', 'Under', 'Pause']:
            data = data.merge(data_metric, left_index=True, right_index=True)
        else:
            # data = data.merge(data_metric.astype(float), left_index=True, right_index=True)
            data = data.merge(data_metric.apply(to_numeric, errors='coerce'), left_index=True, right_index=True)

    f_weight = metadata['Frequency Weighting']

    # Rename percentile column headers using metadata
    for i in range(5):
        percentile = metadata['Percentile ' + str(i + 1)]
        data.columns = data.columns. \
            str.replace('LN' + str(i + 1), 'L' + str(f_weight) + str(percentile).zfill(2))

    # Tidy up column headers

    col_rename = {}
    for col_in in data.columns:

        col_out = col_in.\
            replace(' ', '_').\
            replace('Leq', 'L' + str(f_weight) + 'eq').\
            replace('LE', 'L' + str(f_weight) + 'E').\
            replace('Lmin', 'L' + str(f_weight) + 'min').\
            replace('Lmax', 'L' + str(f_weight) + 'max')
        
        c_split = col_out.split('_')
        
        if 'kHz' in c_split:
            idx_f = c_split.index('kHz') - 1
            c_split[idx_f] = str(int(float(c_split[idx_f]) * 1000))
            c_split[idx_f + 1] = c_split[idx_f + 1].replace('k', '')
            col_out = '_'.join(c_split)

        col_rename[col_in] = col_out

    data.rename(columns=col_rename, inplace=True)
    data.drop(columns=data.filter(regex='Unnamed').columns, inplace=True)

    return data.set_index('Time')   # , f_weight


def duo(file_in, metadata, spectral):

    # Read in data from all worksheets as dictionary
    try:
        data_dict = read_excel(
            file_in, skiprows=8, index_col='Period start', skipfooter=1,
            parse_dates=True, date_parser=lambda x: datetime.strptime(x, "%d/%m/%y %H:%M:%S:%f"),
            sheet_name=None
        )
    except TypeError:
        data_dict = read_excel(
            file_in, skiprows=8, index_col='Period start', skipfooter=1,
            parse_dates=True,
            sheet_name=None
        )

    # Collate into single DataFrame
    data = DataFrame()
    for d in data_dict.values():
        data = data.merge(d, left_index=True, right_index=True, how='outer')

    if spectral:

        # Read in multi-level column headers from all worksheets as dictionary
        headers_dict = read_excel(file_in, skiprows=6, index_col=0, nrows=0, header=[2, 0], sheet_name=None)

        # Collate into single DataFrame
        headers = DataFrame(columns=MultiIndex(levels=[[], []], codes=[[], []]))
        for h in headers_dict.values():
            headers = headers.merge(h, left_index=True, right_index=True, how='outer')

        # Update DataFrame's headers
        data.columns = headers.columns.map('_'.join)

    # Tidy up column headers

    f_weight = metadata['Frequency Weighting']
    col_rename = {}
    for col_in in data.columns:

        col_out = col_in. \
            replace(' ', '_'). \
            replace('_Leq', ''). \
            replace('L', 'L' + str(f_weight)). \
            replace('Hz', '_Hz'). \
            replace('k_Hz', '_kHz'). \
            replace('1/3_', ''). \
            replace('Oct_', ''). \
            replace('__', '_')

        c_split = col_out.split('_')

        if 'kHz' in c_split:
            idx_f = c_split.index('kHz') - 1
            c_split[idx_f] = str(int(float(c_split[idx_f]) * 1000))
            c_split[idx_f + 1] = c_split[idx_f + 1].replace('k', '')
            col_out = '_'.join(c_split)

        if ('Hz' not in c_split) and ('kHz' not in c_split):
            col_out += '_Main'

        col_rename[col_in] = col_out

    data.rename(columns=col_rename, inplace=True)
    data.index.name = 'Time'

    data['Duration'] = data.index
    data['Duration'] = to_timedelta(data['Duration'].shift(-1) - data.index)

    return data


def custom_csv(file_in, columns):

    idx = columns['Time']
    data = read_csv(file_in, usecols=columns.values(), index_col=idx, parse_dates=True)

    columns_inv = {v: k for k, v in columns.items()}
    data = data.rename(columns=columns_inv)
    data.index = data.index.rename('Time')

    return data


def custom_excel(file_in, columns):

    idx = columns['Time']
    data = read_excel(file_in, usecols=columns.values(), index_col=idx, parse_dates=True)

    columns_inv = {v: k for k, v in columns.items()}
    data = data.rename(columns=columns_inv)
    data.index = data.index.rename('Time')

    return data

