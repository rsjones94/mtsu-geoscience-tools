from datetime import date, datetime

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


with open(data_file) as f:
    content = f.readlines()

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

for key, val in data.items():
    print(f'site: {site}. {len(val)} entries')
    standard = len(val[0])
    for line in val:
        assert len(line) == standard
    print('validated')
