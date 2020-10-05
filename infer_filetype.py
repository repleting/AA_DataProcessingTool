# Partially read input file to determine its type:
#   - NL-32, NL-52
#   - Metadata, Data

from pandas import read_excel


def infer(file_in):

    if file_in.lower().endswith('xlsx'):

        try:
            duo_head = read_excel(file_in, nrows=4)
            if str(duo_head.iloc[3, 2]).lower().startswith('duo'):
                return 'duo_octave_metadata'
            elif str(duo_head.iloc[3, 1]).lower().startswith('duo'):
                return 'duo_metadata'
            else:
                return 'custom_excel'
        except IndexError:
            return 'custom_excel'

    first_line = open(file_in).readline()

    if file_in.lower().endswith('rnh') and first_line.lower().startswith('file'):
        return 'nl32_metadata'
    elif file_in.lower().endswith('rnh') and first_line.lower().startswith('csv'):
        return 'nl52_metadata'
    elif file_in.lower().endswith('rnd') and first_line.lower().startswith('address'):
        return 'nl32_data'
    elif file_in.lower().endswith('rnd') and first_line.lower().startswith('csv'):
        return 'nl52_data'
    elif file_in.lower().endswith('csv'):
        return 'custom_csv'
    else:
        return 'unknown'
