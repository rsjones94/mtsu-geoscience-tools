import os
from datetime import date, datetime, timedelta
from copy import deepcopy

import pandas as pd

path_nodata = r'C:\Users\rj3h\Desktop\rdb\parsed_nodata'
path_999 = r'C:\Users\rj3h\Desktop\rdb\parsed_999'
data_file = r'C:\Users\rj3h\Desktop\rdb\dv1.txt'
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
    "00400": "pH",
    "00060": "Discharge",
    "72137": "Discharge_fil",
    "00065": "Gage",
    "00070": "Turb_Jackson",
    "00076": "Turb_neph",
    "63680": "Turb_FNU",
    "00300": "DO_mgL",
    "00301": "DO_percent",
    "00630": "N_unfiltered",
    "00631": "N_filtered",
    "99133": "N_insitu",
    "99137": "Nitrate_insitu",
    "80154": "SS_mgL",
    "80155": "SS_shorttonsperday",
    "80297": "SS_regression1",
    "99409": "SS_regression2",
    }
stat_meanings = {
    '00001': 'max',
    '00002': 'min',
    '00003': 'mean'
    }
giv_cols = []
for i in parameter_meanings.values():
    for j in stat_meanings.values():
        giv_cols.append(i+'_'+j)
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

for key, val in ddfs.items():
    print(key, len(val), val.iloc[0].name, val.iloc[-1].name)

# add missing cols and reorder
for key, val in ddfs.items():
    print(f'adding cols and reordering: {key}')
    for c in giv_cols:
        if c not in ddfs[key]:
            ddfs[key][c] = None

    ddfs[key] = ddfs[key][giv_cols]

for key, val in ddfs.items():
    print(f'Writing {key}')
    val.index.name = 'Date'
    val = val.replace(to_replace=[None], value='NoData')
    val.to_csv(os.path.join(path_nodata,
                            key+'.csv'))
    val = val.replace(to_replace=['NoData'], value=-999)
    val.to_csv(os.path.join(path_999,
                            key+'.csv'))
