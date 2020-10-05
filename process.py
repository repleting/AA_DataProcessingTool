# Data processing modules

from numpy import log10
from pandas import date_range, DataFrame, to_timedelta
from pandas.tseries.offsets import BusinessHour, Hour
from datetime import datetime


def regularise_noise(data, args):

    """

    Returns regularised DataFrame:
        1. Snap datetime index to output resolution
        2. Remove duplicated datetimes
        3. Pad missing entries with blanks

    args = [resolution, drop_ends]
        - resolution (pandas offset alias): output resolution of data
        - drop_ends (bool): if True, removes first and last data points

    """

    resolution, drop_ends = args

    if type(resolution) == int:
        resolution = str(resolution) + "T"

    # Snap to resolution
    data.index = data.index.round(resolution)

    # Remove duplicates and count bad entries
    data_tmp = data.reset_index().drop_duplicates(subset='Time', keep=False).set_index('Time')
    count_duplicates = data.shape[0] - data_tmp.shape[0]
    data = data_tmp

    # Pad missing entries
    pad_idx = date_range(data.index.min(), data.index.max(), freq=resolution, name='Time')
    data = data.resample(resolution).first().reindex(pad_idx)

    if drop_ends.lower() == "true":
        data = data.drop(index=data.index[0]).drop(index=data.index[-1])

    return data, count_duplicates


def resample_noise(data, args):

    """

    TODO: check incorporation of metadata for percentiles and freq weighting

    Returns re-sampled DataFrame

    data = Input DataFrame
    args = [res_out, percentiles, f_weight, max_remove, avg_type]

        - res_out           : output resolution, e.g. '1D' for one day
        - max_remove        : for Lmax re-sampling, number of highest entries to ignore in each output period
        - avg_type          : type of averaging for percentiles; can be mean, median or mode
        - f_weight          : frequency weighting
        - percentiles       : list of percentile values in input

    Uses beginning of column name to determine re-sampling method, as follows (for f_weight = 'A'):
        - 'LAeq'            : log average
        - 'LAmin'           : minimum
        - 'LAmax'           : maximum, after removing highest max_remove entries
        - 'LA##'            : linear average, mean of mode or lower quartile (depends on args[4])
        - 'LAE'             : log sum
        - 'End_Time'        : last
        -  None of the above: first

    """

    res_out, max_remove, avg_type, f_weight, percentiles, leq_avg = args

    if type(res_out) == int:
        res_out = str(res_out) + 'T'

    if avg_type not in ['mean', 'median', 'mode', 'lq']:
        raise Exception("Average type for percentile re-sampling must be mean, median, mode or lq (lower quartile)")
    if leq_avg not in ['linear', 'log']:
        raise Exception("Average type for Leq re-sampling must be linear or log")

    cols = data.columns
    resamp_idx = date_range(data.index.min().floor(res_out), data.index.max(), freq=res_out, name='Time')
    data_out = DataFrame()

    # Count missing samples
    freq_in, freq_out = data.index.freq, resamp_idx.freq

    if freq_in == BusinessHour():
        freq_in = Hour()
    if freq_out == BusinessHour():
        freq_out = Hour()

    n = freq_out / freq_in

    if n >= 1:
        data_tmp = DataFrame(index=data.index)
        data_tmp['Missing Samples'] = 1
        data_tmp = n - data_tmp.filter(regex='Missing Samples').resample(res_out).sum().reindex(resamp_idx)
        data_out = data_out.merge(data_tmp, left_index=True, right_index=True, how='outer')
    else:
        data_out['Missing Samples'] = 0

    # Leq dependant on input arguments

    # Log average for Leq (if used as pre-processing module)
    if leq_avg == 'log':

        data_tmp = 10**(data.filter(regex='^L' + f_weight + 'eq')/10)

        if len(data_tmp.columns) > 0:
            data_tmp = data_tmp.resample(res_out).mean().reindex(resamp_idx)
            data_tmp = 10 * log10(data_tmp)
            data_out = data_out.merge(data_tmp, left_index=True, right_index=True, how='outer')

    # Linear average for Leq (if used for summary table generation)
    elif leq_avg == 'linear':

        data_tmp = data.filter(regex='^L' + f_weight + 'eq')

        if len(data_tmp.columns) > 0:
            data_tmp = data_tmp.resample(res_out).mean().reindex(resamp_idx)
            data_out = data_out.merge(data_tmp, left_index=True, right_index=True, how='outer')

    # Minimum for Lmin / Start Time
    cols_tmp = data.filter(regex='^L' + f_weight + 'min').columns.to_list()
    data_tmp = data[cols_tmp].resample(res_out).min().reindex(resamp_idx)

    if len(data_tmp.columns) > 0:
        data_out = data_out.merge(data_tmp, left_index=True, right_index=True, how='outer')

    # Maximum for Lmax (after excluding highest max_remove entries in each re-sampled period)
    data_tmp = data.filter(regex='^L' + f_weight + 'max')

    if len(data_tmp.columns) > 0:
        data_tmp = data_tmp.resample(res_out).transform(lambda x: x.nlargest(max_remove + 1).min())
        data_tmp = data_tmp.resample(res_out).max().reindex(resamp_idx)
        data_out = data_out.merge(data_tmp, left_index=True, right_index=True, how='outer')

    # Percentiles
    #  - Perhaps output histograms to inform choices
    for p in percentiles:

        data_tmp = data.filter(regex='^L' + f_weight + str(p).zfill(2))

        if len(data_tmp.columns) > 0:

            if avg_type == 'mean':
                data_tmp = data_tmp.resample(res_out).mean().reindex(resamp_idx)
            elif avg_type == 'mode':
                data_tmp = data_tmp.resample(res_out).apply(lambda x: x.round(0).mode().min()).reindex(resamp_idx)
            elif avg_type == 'median':
                data_tmp = data_tmp.resample(res_out).median().reindex(resamp_idx)
            elif avg_type == 'lq':
                data_tmp = data_tmp.resample(res_out).quantile(0.25).reindex(resamp_idx)

            data_out = data_out.merge(data_tmp, left_index=True, right_index=True, how='outer')

    # Log sum for LE
    data_tmp = 10**(data.filter(regex='^L' + f_weight + 'E')/10)

    if len(data_tmp.columns) > 0:
        data_tmp = data_tmp.resample(res_out).sum().reindex(resamp_idx)
        data_tmp = 10 * log10(data_tmp)
        data_out = data_out.merge(data_tmp, left_index=True, right_index=True, how='outer')

    # Last for End Time
    if 'End_Time' in data.columns:
        data_tmp = data[['End_Time']].resample(res_out).last().reindex(resamp_idx)
        data_out = data_out.merge(data_tmp, left_index=True, right_index=True, how='outer')

    # First for everything else
    remainder = []
    for c in cols:
        if c not in data_out.columns:
            remainder.append(c)

    data_tmp = data[remainder].resample(res_out).first().reindex(resamp_idx)

    if len(data_tmp.columns) > 0:
        data_out = data_out.merge(data_tmp, left_index=True, right_index=True, how='outer')

    data_out['Duration'] = to_timedelta(res_out)
    # What are columns [*, Over, Under, Pause] and how do we deal with them?

    return data_out[cols], 'No auxiliary data'


def flag_periods(data, args):

    """

    Returns DataFrame with additional columns to flag whether sample is in user-defined periods
    Periods can be overlapping, but start time is included, end time excluded (i.e. {t: t1 <= t < t2})

    data = Input DataFrame
    args = [Name, StartTime, EndTime, Recurrence]

        - Name             : string denoting name of first period
        - StartTime        : starting date-time of first period as datetime object
        - EndTime          : ending date-time of first period as datetime object
        - Recurrence       : False for one-off event,
                             or list of integers representing applicable days for first period, e.g.:
                                   [0, 1, 2, 3, 4, 5, 6]   : every day
                                   [0, 1, 2, 3, 4]         : every weekday
                                   [5, 6]                  : weekends

    NOTE: if a recurrence is used with an overnight period (crossing midnight), samples on selected days are flagged

    Example for weekend night-times:

        StartTime   = datetime.datetime(2019, 7, 18, 23, 0) - start at 11pm
        EndTime     = datetime.datetime(2019, 7, 19, 7, 0)  - finish at 7am
        Recurrence  = range(5, 7)   - apply to weekends (Saturday & Sunday)

        Output will flag the following time periods:
            Saturday    [00:00, 07:00)
            Saturday    [23:00, 00:00)
            Sunday      [00:00, 07:00)
            Sunday      [23:00, 00:00)

        To flag Friday nights and Sunday mornings with this example, two extra periods will need to be defined:
            {
                'Friday Night':     [23:00, 00:00, 4]
                'Weekend Nights':   [23:00, 07:00, [5, 6]]
                'Sunday Morning':   [00:00, 07:00, 6]
            }

    """

    period_name = args[0]
    t_start = datetime.strptime(args[1], '%d/%m/%y %H:%M')
    t_end = datetime.strptime(args[2], '%d/%m/%y %H:%M')

    # if args[3] == 'False':
    #     recurrence = False
    # else:
    #     rec_tmp = args[3].replace(' ', '').split(',')
    #     recurrence = []
    #     for r in rec_tmp:
    #         recurrence.append(int(r))

    recurrence = args[3]

    if recurrence:

        if t_end.time() > t_start.time():

            data['Flag_' + period_name] = \
                (data.index.weekday.isin(recurrence)) & \
                (data.index.time >= t_start.time()) &\
                (data.index.time < t_end.time())

        else:

            # Overnight cases
            data['Flag_' + period_name] = \
                (data.index.weekday.isin(recurrence)) & \
                (
                        (data.index.time >= t_start.time()) |
                        (data.index.time < t_end.time())
                )

    else:

        data['Flag_' + period_name] = \
            (data.index >= t_start) & \
            (data.index < t_end)

    # Replace bool with start time
    data['Flag_' + period_name] = data['Flag_' + period_name].replace({True: t_start, False: None})

    return data, 'No auxiliary data'


def remove_periods(data, args):

    """

    Returns DataFrame after removing all entries falling within specified time periods

    Inputs:
        data = Input DataFrame
        args = List of time periods to remove, as specified in flag_periods()

    Returns:
        df_out:     Output DataFrame
        drop_count: Number of entries removed

    """

    args = ['Flag_' + f for f in args]
    # df_out = data[data[args].isnull().sum(axis=1) == len(args)]
    df_out = data.copy()
    df_out[df_out[args].isnull().sum(axis=1) != len(args)] = None
    drop_count = len(data) - len(df_out)

    return df_out, drop_count


def third_to_octave(data, _):

    """

    Converts third-octave data into octaves by taking logarithmic sum of 3 bands around central
    No arguments required

    Inputs:
        data: Input DataFrame with third-octave columns for each metric
        args: Dummy argument

    Returns:
        df:     Output DataFrame, reduced to octaves for each metric
        df_aux: DataFrame showing which frequency bands contribute to each octave band

    Assumptions:

        1. Input data contains 3n+1 columns for each metric
        2. Spectral columns are in ascending order, with octave value in the centre of each group of 3

        e.g. for LAeq, n=2:

            - LAeq_Main
            ------------------------------
            - LAeq_12.5_Hz
            - LAeq_16_Hz    - octave value
            - LAeq_20_Hz
            ------------------------------
            - LAeq_25_Hz
            - LAeq_31.5_Hz  - octave value
            - LAeq_40_Hz

    """

    df = data.copy()
    df_aux = DataFrame(columns=['Output_Band', 'Input_1', 'Input_2', 'Input_3'])
    metrics = df.filter(regex='Main').columns.to_list()

    for i, m in enumerate(metrics):

        df_tmp = 10 ** (df.filter(regex=m.replace('_Main', '') + '.*Hz') / 10)
        cols = df_tmp.columns.to_list()

        for j, c in enumerate(cols[1::3]):

            c0 = cols[3*j]
            c1 = cols[3*j + 2]

            df_tmp[c] = df_tmp[c0] + df_tmp[c] + df_tmp[c1]
            df_tmp.drop(columns=[c0, c1], inplace=True)
            df.drop(columns=[c0, c1], inplace=True)

            df_aux.loc[m.replace('_Main', '')] = [c, c0, c, c1]

        df_tmp = 10 * log10(df_tmp)

        for j, c in enumerate(cols[1::3]):
            df[c] = df_tmp[c]

    return df, df_aux


def process_batch(data, modules, metadata):

    module_names = {
        "Regularise": regularise_noise,
        "Re-sample": resample_noise,
        "Flag time": flag_periods,
        "Remove time": remove_periods,
        "Convert to octaves": third_to_octave
    }

    print("\nPre-processing data...")
    data_aux = []

    for i, mod in enumerate(modules):
        print('    Module ' + str(i + 1) + ' of ' + str(len(modules)) + ': ' + mod[0])

        mod_func = module_names[mod[0]]
        mod_args = mod[1].copy()

        # Incorporate frequency weighting and percentiles from metadata in the case of re-sampling
        if mod[0] == 'Re-sample':
            mod_args.append(metadata["Frequency Weighting"])
            mod_args.append(list(map(float, metadata.filter(regex='Percentile').to_list())))
            mod_args.append('log')

        # Run module
        data, aux_tmp = mod_func(data, mod_args)
        # Collate auxiliary data
        data_aux.append(aux_tmp)

    print("Pre-processing successful")

    print("\nSummary:")
    for mod in modules:
        print('    ' + str(mod))
    print("\n")

    return data, data_aux

