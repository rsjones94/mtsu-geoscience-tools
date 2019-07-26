import os
from datetime import date, datetime, timedelta
from copy import deepcopy

import pandas as pd

data_file = r'C:\Users\rsjon_000\Documents\mtsu-geoscience-tools\rdb-parsing\data\dv2.txt'
"""
parameter_meanings = {
    "00400": "pH, water, unfiltered, field, standard units",
    "00060": "Discharge, cubic feet per second",
    "72137": "Discharge, tidally filtered, cubic feet per second",
    "00065": "Gage height, feet",
    "00070": "Turbidity, water, unfiltered, Jackson Turbidity Units",
    "00076": "Turbidity, water, unfiltered, nephelometric turbidity units",
    "63680": "Turbidity, water, unfiltered, monochrome near infra-red LED light, 780-900 nm, detection angle 90 +-2.5 degrees, formazin nephelometric units (FNU)",
    "00300": "Dissolved oxygen, water, unfiltered, milligrams per liter",
    "00301": "Dissolved oxygen, water, unfiltered, percent of saturation",
    "00630": "Nitrate plus nitrite, water, unfiltered, milligrams per liter as nitrogen",
    "00631": "Nitrate plus nitrite, water, filtered, milligrams per liter as nitrogen",
    "99133": "Nitrate plus nitrite, water, in situ, milligrams per liter as nitrogen",
    "99137": "Nitrate, water, in situ, milligrams per liter as nitrogen",
    "80154": "Suspended sediment concentration, milligrams per liter",
    "80155": "Suspended sediment discharge, short tons per day",
    "80297": "Suspended sediment load, water, unfiltered, computed, the product of regression-computed suspended sediment concentration and streamflow, short tons per day",
    "99409": "Suspended sediment concentration, water, unfiltered, estimated by regression equation, milligrams per liter",
    }
"""
parameter_meanings = {
    "00400": "pH_water_unfiltered_field_standard units",
    "00060": "Discharge_cfs",
    "72137": "Discharge_tidally_filtered_cfs",
    "00065": "Gage_height_feet",
    "00070": "Turbidity_water_unfiltered_JTU",
    "00076": "Turbidity_ water_unfiltered_NTU",
    "63680": "Turbidity_water_FNU",
    "00300": "Dissolved_oxygen_mpl",
    "00301": "Dissolved_oxygen_percsat",
    "00630": "Nitrate_plus_nitrite_unfiltered_mpl",
    "00631": "Nitrate_plus_nitrite_filtered_mpl",
    "99133": "Nitrate_plus_nitrite_insitu_mpl",
    "99137": "Nitrate_water_insitu_mpl",
    "80154": "Suspended_sediment_concentration_mpl",
    "80155": "Suspended_sediment_discharge_shtd",
    "80297": "Suspended_sediment_load_computed_shtd",
    "99409": "Suspended_sediment_concentration_regression_mpl",
    }
stat_meanings = {
    '00001': 'Maximum',
    '00002': 'Minimum',
    '00003': 'Mean'
    }
date_range = (date(1990, 1, 1), date(2019, 4, 30))


def parse_parstat(parstat):

    subs = parstat.split('_')
    if len(subs) == 1 or len(subs) == 4:
        return 'unneeded'
    try:
        par = parameter_meanings[subs[1]]
        stat = stat_meanings[subs[2]]
    except KeyError:
        return 'unneeded'
    return par+'_'+stat


def insert_placeholder(index, l):
    out = l.copy()
    out.insert(index+2, 'X')
    out.insert(index+1, None)
    return out


def repair_missing_data(l):
    new_list = l.copy()
    for i, el in enumerate(l):
        if new_list[i] == '\t' and new_list[i+1] == '\t':
            new_list = insert_placeholder(i, new_list)
            new_list = repair_missing_data(new_list)
            break

    return new_list


def rdbline_to_list(line, repair=True, lop=False):
    """
    Delimit using escape chars and but also keep those chars.

    The problem with the provided format is this:
        Data that IS provided is followed with tab-X-tab
        Data that is missing is not given a placeholder, nor does X show up
            Thus it is represented by tab-tab
        So a column with data is data-tab-a-tab
        A column without is tab-tab
        So if a tab-tab is detected, insert a placeholder before the tabs and an A between
    """
    new_list = []
    builder = ''
    for i in line:
        if i == '\t' or i == '\n':
            if builder != '':
                new_list.append(builder)
            new_list.append(i)
            builder = ''
        else:
            builder += i
    if repair:
        new_list = repair_missing_data(new_list)
    new_list = [a for a in new_list if a not in ['\t', '\n']]
    if lop:
        new_list = new_list[2:]
        new_list[0] = datetime.strptime(new_list[0], '%Y-%m-%d')
        for i, entry in enumerate(new_list):
            try:
                new_list[i] = float(entry)
            except TypeError:
                pass
    return new_list


###########################

# read raw data
with open(data_file) as f:
    content = f.readlines()

# parse it
data = {}
recording = False
for i, line in enumerate(content):
    if 'agency_cd' in line:
        recording = True
        start = i
        cols = rdbline_to_list(line, repair=True, lop=False)
        other_cols = cols[:3]
        num_cols = cols[3:]
        num_cols = [parse_parstat(a) for a in num_cols]
        all_cols = other_cols
        all_cols.extend(num_cols)
        site = rdbline_to_list(content[i+2], repair=True, lop=False)[1]
        data[site] = [all_cols]
        print(f'Collecting {site}')
        continue
    if recording and i != start+1:
        rep_line = rdbline_to_list(line)
        try:
            if rep_line[1] != site:
                recording = False
            else:
                data[site].append(rep_line)
        except IndexError:
            recording = False

# validate and convert
dfs = {}
for key, val in data.items():
    print(f'site: {key}. {len(val)} entries')
    standard = len(val[0])
    for line in val:
        assert len(line) == standard
    print('validated. converting to df')
    dfs[key] = pd.DataFrame.from_records(val[1:], columns=val[0])

    # we only want to keep relevant columns
    keep_cols = [c for c in dfs[key].columns if c != 'unneeded']
    dfs[key] = dfs[key][keep_cols]
    # convert str to date
    dfs[key]['datetime'] = pd.to_datetime(dfs[key]['datetime']).dt.date


doer = 15
ex = 0
ddfs = {}
dfs_copy = deepcopy(dfs)
for key, val in dfs_copy.items():
    ddfs[key] = val
    print(f'fixing dates for {key}')
    ddfs[key] = dfs_copy[key].set_index('datetime')
    d_first = ddfs[key].iloc[0].name
    d_last = ddfs[key].iloc[-1].name
    cols = ddfs[key].columns.values
    blank = pd.Series([None for c in cols], index=cols, name=None)
    print(f'blank len is {len(blank)}. ncol is {len(ddfs[key].columns)}')

    """
    d_index = date_range[0]
    print(f'adding front. go from {d_index} to {d_first}')
    while d_index < d_first:
        blank.name = d_index
        try:
            ddfs[key] = ddfs[key].append(blank)
        except ValueError:
            print(f"--------------Can't append {d_index}")
            pass
        d_index = d_index + timedelta(days=1)

    d_index = d_last
    print(f'adding back. go from {d_index} to {date_range[1]}')
    while d_index < date_range[1] + timedelta(days=1):
        blank.name = d_index
        try:
            ddfs[key] = ddfs[key].append(blank)
        except ValueError:
            print(f"--------------Can't append {d_index}")
            pass
        d_index = d_index + timedelta(days=1)
    """
    fr = date_range[0]
    to = d_first - timedelta(days=1)
    if fr < to:
        print(f'adding front. go from {fr} to {to}')
        delta = timedelta(days=(to-fr).days)
        index = pd.date_range(to - delta, periods=delta.days, freq='D')
        front_df = pd.DataFrame(index=index, columns=cols)
        try:
            ddfs[key] = ddfs[key].append(front_df)
        except ValueError:
            print('cannot append')

    fr = d_last + timedelta(days=1)
    to = date_range[1] + timedelta(days=1)
    if fr < to:
        print(f'adding back. go from {fr} to {to}')
        delta = timedelta(days=(to-fr).days)
        index = pd.date_range(to - delta, periods=delta.days, freq='D')
        front_df = pd.DataFrame(index=index, columns=cols)
        try:
            ddfs[key] = ddfs[key].append(front_df)
        except ValueError:
            print('cannot append')

    # sort it
    ddfs[key].index = pd.to_datetime(ddfs[key].index).date
    ddfs[key] = ddfs[key].sort_index()
    # slice the datetime range
    ddfs[key] = ddfs[key].loc[date_range[0]:date_range[1]]
    # remove duplicates
    ddfs[key] = ddfs[key][~ddfs[key].index.duplicated()]

    """
    ex += 1
    if ex == doer:
        break
    """

for key, val in ddfs.items():
    print(key, len(val), val.iloc[0].name, val.iloc[-1].name)

i = 0
maxi = 30
for key, val in ddfs.items():
    val.to_csv(os.path.join(r'C:\Users\rsjon_000\Documents\mtsu-geoscience-tools\rdb-parsing\data\out',
                            key+'.csv'))
    i += 1
    if i == maxi:
        break
