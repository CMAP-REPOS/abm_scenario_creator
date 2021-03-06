#!/usr/bin/env python
'''
    tmm_gdb2csv.py
    Author: npeterson
    Revised: 4/9/2015
    ---------------------------------------------------------------------------
    This script will use the extra attribute tables in TMM_GIS.gdb to create
    updated versions of the batchin CSVs used to construct the transit network
    for the Transit Modernization Model. A new network can then be constructed
    using the altered batchin files to model the transit improvements.

'''
import os
import sys
import arcpy
import csv
import TMM

# -----------------------------------------------------------------------------
#  Set parameters.
# -----------------------------------------------------------------------------
input_dir = TMM.input_dir
output_dir = TMM.ensure_dir(TMM.output_dir)

scen = 100  # Year 2010
tod_periods = range(1, 9)  # 1-8

tline_table = os.path.join(TMM.gdb, 'extra_attr_tlines')
node_table = os.path.join(TMM.gdb, 'extra_attr_nodes')

bus_node_attr_csv_in = os.path.join(input_dir, 'bus_node_extra_attributes.csv')
rail_node_attr_csv_in = os.path.join(input_dir, 'rail_node_extra_attributes.csv')
tline_easeb_csv_in = os.path.join(input_dir, 'boarding_ease_by_line_id.csv')
tline_prof_csv_in = os.path.join(input_dir, 'productivity_bonus_by_line_id.csv')
tline_relim_csv_in = os.path.join(input_dir, 'relim_by_line_id.csv')

bus_node_attr_csv_out = bus_node_attr_csv_in.replace(input_dir, output_dir)
rail_node_attr_csv_out = rail_node_attr_csv_in.replace(input_dir, output_dir)
tline_easeb_csv_out = tline_easeb_csv_in.replace(input_dir, output_dir)
tline_prof_csv_out = tline_prof_csv_in.replace(input_dir, output_dir)
tline_relim_csv_out = tline_relim_csv_in.replace(input_dir, output_dir)


# -----------------------------------------------------------------------------
#  Define functions.
# -----------------------------------------------------------------------------
def adjust_easeb_value(tline_id, tline_dict, csv_dict):
    ''' Create a composite score from a subset of the GDB tline table's fields,
        to provide a boost to the @easeb extra attribute. '''
    # Get current easeb value
    current_easeb_value = float(csv_dict[tline_id]['@easeb'])
    max_easeb_value = 4.0  # 3 = 'level w/ platform'

    # Update easeb values for tlines in GDB that could be improved
    if tline_id in tline_dict and current_easeb_value < max_easeb_value:

        # Set field scale factors (1 / maximum field value) & score weights
        field_fwv = {
            'ADD_STAND_CAP': {'f': 1.0/5, 'w': 1.0},
            'LOWER_FLOOR':   {'f': 1.0/3, 'w': 1.0},
            'NEW_VEHICLES':  {'f': 1.0/5, 'w': 1.0},
        }

        # Get current field values
        for attr in field_fwv.iterkeys():
            field_fwv[attr]['v'] = float(tline_dict[tline_id][attr])

        # Calculate node's improvement score (0-2)
        node_improvement = sum([field_fwv[attr]['v'] * field_fwv[attr]['f'] * field_fwv[attr]['w'] for attr in field_fwv.iterkeys()])
        max_improvement = sum([field_fwv[attr]['w'] for attr in field_fwv.iterkeys()])
        pct_improvement = node_improvement / max_improvement

        # Calculate adjusted easeb value
        max_adjustment = max_easeb_value - current_easeb_value
        adjustment = max_adjustment * pct_improvement  # The higher the current @easeb, the harder it is to improve
        adjusted_easeb_value = round(current_easeb_value + adjustment, 2)  # NOT AN INTEGER

        # Set adjusted easeb value
        csv_dict[tline_id]['@easeb'] = str(adjusted_easeb_value)
        return str(adjusted_easeb_value)

    # Ignore tlines not in GDB and maxed-out-@easeb tlines
    else:
        return str(current_easeb_value)


def adjust_info_value(node_id, node_dict, csv_dict, info_field):
    ''' Check existing real-time info value and update if appropriate. '''
    # Get current real-time info (@bsinf/@rsinf) value
    current_info_value = csv_dict[node_id][info_field]

    # Update info values for nodes in GDB w/o real-time info already
    if node_id in node_dict and current_info_value != '2':
        add_info = node_dict[node_id]['ADD_INFO']
        add_pa = node_dict[node_id]['ADD_PA']
        if add_info + add_pa > 0:
            adjusted_info_value = '2'
        else:
            adjusted_info_value = current_info_value

        # Set adjusted info value
        csv_dict[node_id][info_field] = adjusted_info_value
        adjusted_info_value = current_info_value
        return adjusted_info_value

    # Ignore nodes not in GDB or w/ real-time info already
    else:
        return current_info_value


def adjust_prof_values(tline_id, tline_dict, csv_dict):
    ''' Check existing productivity bonus values and update if appropriate. '''
    # Get current @prof1-3 values
    current_prof1_value = float(csv_dict[tline_id]['@prof1'])
    current_prof2_value = float(csv_dict[tline_id]['@prof2'])
    current_prof3_value = float(csv_dict[tline_id]['@prof3'])
    current_prof_values = (str(current_prof1_value), str(current_prof2_value), str(current_prof3_value))

    # Update @prof1-3 values for tlines in GDB
    if tline_id in tline_dict:

        # Set field scale factors (1 / maximum field value) & bonus weights
        field_fwv = {
            'ADD_WIFI':  {'f': 1.0/1, 'w': 0.05},
            'IMP_SEATS': {'f': 1.0/5, 'w': 0.05},
        }

        # Get current field values
        for attr in field_fwv.iterkeys():
            field_fwv[attr]['v'] = int(tline_dict[tline_id][attr])

        # Calculate productivity bonuses
        wifi_bonus = field_fwv['ADD_WIFI']['v'] * field_fwv['ADD_WIFI']['f'] * field_fwv['ADD_WIFI']['w']
        seat_bonus = field_fwv['IMP_SEATS']['v'] * field_fwv['IMP_SEATS']['f'] * field_fwv['IMP_SEATS']['w']
        productivity_bonus = wifi_bonus + seat_bonus

        # Set adjusted @prof1-3 values
        adjusted_prof1_value = round(current_prof1_value - productivity_bonus, 2)
        adjusted_prof2_value = round(current_prof2_value - productivity_bonus, 2)
        adjusted_prof3_value = round(current_prof3_value - productivity_bonus, 2)
        csv_dict[tline_id]['@prof1'] = str(adjusted_prof1_value)
        csv_dict[tline_id]['@prof2'] = str(adjusted_prof2_value)
        csv_dict[tline_id]['@prof3'] = str(adjusted_prof3_value)
        adjusted_prof_values = (str(adjusted_prof1_value), str(adjusted_prof2_value), str(adjusted_prof3_value))

        return adjusted_prof_values

    # Ignore tlines not in GDB
    else:
        return current_prof_values


def adjust_relim_value(tline_id, tline_dict, csv_dict):
    ''' Decrease reliability impact in proportion to specified reliability improvement. '''
    # Get current reliability impact (@relim) value
    current_relim_value = float(csv_dict[tline_id]['@relim'])

    # Caluclate the updated value for tlines in GDB
    if tline_id in tline_dict:
        imp_rel_int = tline_dict[tline_id]['IMP_RELIABILITY']
        imp_rel_pct = imp_rel_int / 100.0
        adjusted_relim_value = round(current_relim_value * (1.0 - imp_rel_pct), 2)
        csv_dict[tline_id]['@relim'] = adjusted_relim_value

        return str(adjusted_relim_value)

    # Ignore tlines not in GDB
    else:
        return str(current_relim_value)


def adjust_rspac_value(node_id, node_dict, csv_dict):
    ''' Add new parking spaces to nodes. RAIL STATIONS ONLY! '''
    # Get current parking spaces (@rspac) count
    current_rspac_value = int(csv_dict[node_id]['@rspac'])

    # Caluclate the updated value for nodes in GDB
    if node_id in node_dict:
        new_parking = node_dict[node_id]['ADD_PARKING']
        adjusted_rspac_value = max(current_rspac_value + new_parking, 0)  # Don't allow net-negative values
        csv_dict[node_id]['@rspac'] = adjusted_rspac_value

        return str(adjusted_rspac_value)

    # Ignore nodes not in GDB
    else:
        return str(current_rspac_value)


def adjust_type_value(node_id, node_dict, csv_dict, type_field):
    ''' Create a composite score from a subset of the GDB node table's fields,
        to provide a boost to the @bstyp/@rstyp extra attributes. '''
    # Get current type value
    current_type_value = float(csv_dict[node_id][type_field])
    max_type_value = 6.0  # 5 = 'major terminal'

    # Update station/stop type (@bstyp/@rstyp) values for nodes in GDB that could be improved
    if node_id in node_dict and current_type_value < max_type_value:

        # Set field scale factors (1 / maximum field value) & score weights
        field_fwv = {
            'ADD_ADA':      {'f': 1.0/1, 'w': 1.0},
            'ADD_RETAIL':   {'f': 1.0/1, 'w': 1.0},
            'ADD_SEATS':    {'f': 1.0/5, 'w': 1.0},
            'ADD_SEC_CAM':  {'f': 1.0/5, 'w': 1.0},
            'ADD_SHELTER':  {'f': 1.0/5, 'w': 1.0},
            'ADD_WALKWAY':  {'f': 1.0/1, 'w': 1.0},
            'ENLARGE_AREA': {'f': 1.0/5, 'w': 1.0},
            'FACELIFT':     {'f': 1.0/5, 'w': 1.0},
            'IMP_LIGHTING': {'f': 1.0/5, 'w': 1.0},
            'IMP_WARMING':  {'f': 1.0/5, 'w': 1.0},
        }

        # Get current field values
        for attr in field_fwv.iterkeys():
            field_fwv[attr]['v'] = float(node_dict[node_id][attr])

        # Calculate node's improvement score (0-4)
        node_improvement = sum([field_fwv[attr]['v'] * field_fwv[attr]['f'] * field_fwv[attr]['w'] for attr in field_fwv.iterkeys()])
        max_improvement = sum([field_fwv[attr]['w'] for attr in field_fwv.iterkeys()])
        pct_improvement = node_improvement / max_improvement

        # Calculate adjusted type value
        max_adjustment = max_type_value - current_type_value
        adjustment = max_adjustment * pct_improvement  # The higher the current type, the harder it is to improve
        adjusted_type_value = round(current_type_value + adjustment, 2)  # NOT AN INTEGER

        # Set adjusted type value
        csv_dict[node_id][type_field] = str(adjusted_type_value)
        return str(adjusted_type_value)

    # Ignore nodes not in GDB and maxed-out-@bstyp/@rstyp nodes
    else:
        return str(current_type_value)


def make_dict_from_csv(csv_file_path, id_is_tline=False):
    ''' Read a CSV and construct a dictionary whose keys are the first value in
        each row and whose values are a dictionary of all row values stored by
        fieldname keys. Also returns a list of CSV's fieldnames, in order. '''
    csv_dict = {}
    csv_fields = []
    with open(csv_file_path, 'r') as attr_csv:
        dict_reader = csv.DictReader(attr_csv)
        csv_fields.extend(dict_reader.fieldnames)
        id_field = csv_fields[0]
        for row_dict in dict_reader:
            dict_id = row_dict[id_field] if id_is_tline else int(row_dict[id_field])
            csv_dict[dict_id] = row_dict.copy()
    return csv_dict, csv_fields


def write_dict_to_csv(csv_file, csv_dict, csv_fields, id_is_tline=False):
    ''' Write one of the modified CSV dictionaries and write out an updated
        CSV file with its values. '''
    with open(csv_file, 'wb') as attr_csv:
        dict_writer = csv.DictWriter(attr_csv, csv_fields)
        dict_writer.writeheader()
        sorted_keys = sorted(csv_dict.iterkeys())
        for dict_id in sorted_keys:
            dict_writer.writerow(csv_dict[dict_id])
    return csv_file


# -----------------------------------------------------------------------------
#  Load input GDB tables & CSVs into dictionaries.
# -----------------------------------------------------------------------------
tline_gdb_dict = TMM.make_attribute_dict(tline_table, 'TLINE_ID', TMM.tline_fields)
node_gdb_dict = TMM.make_attribute_dict(node_table, 'NODE_ID', TMM.node_fields)

bus_csv_dict, bus_csv_fields = make_dict_from_csv(bus_node_attr_csv_in)
rail_csv_dict, rail_csv_fields = make_dict_from_csv(rail_node_attr_csv_in)
easeb_csv_dict, easeb_csv_fields = make_dict_from_csv(tline_easeb_csv_in, id_is_tline=True)
prof_csv_dict, prof_csv_fields = make_dict_from_csv(tline_prof_csv_in, id_is_tline=True)
relim_csv_dict, relim_csv_fields = make_dict_from_csv(tline_relim_csv_in, id_is_tline=True)


# -----------------------------------------------------------------------------
#  Update dictionary values to reflect GDB scenario.
# -----------------------------------------------------------------------------
for node_id in bus_csv_dict.iterkeys():
    adjust_type_value(node_id, node_gdb_dict, bus_csv_dict, '@bstyp')
    adjust_info_value(node_id, node_gdb_dict, bus_csv_dict, '@bsinf')

for node_id in rail_csv_dict.iterkeys():
    adjust_type_value(node_id, node_gdb_dict, rail_csv_dict, '@rstyp')
    adjust_info_value(node_id, node_gdb_dict, rail_csv_dict, '@rsinf')
    adjust_rspac_value(node_id, node_gdb_dict, rail_csv_dict)

for tline_id in easeb_csv_dict.iterkeys():
    adjust_easeb_value(tline_id, tline_gdb_dict, easeb_csv_dict)

for tline_id in prof_csv_dict.iterkeys():
    adjust_prof_values(tline_id, tline_gdb_dict, prof_csv_dict)

for tline_id in relim_csv_dict.iterkeys():
    adjust_relim_value(tline_id, tline_gdb_dict, relim_csv_dict)


# -----------------------------------------------------------------------------
#  Write output CSVs.
# -----------------------------------------------------------------------------
write_dict_to_csv(bus_node_attr_csv_out, bus_csv_dict, bus_csv_fields)
write_dict_to_csv(rail_node_attr_csv_out, rail_csv_dict, rail_csv_fields)
write_dict_to_csv(tline_easeb_csv_out, easeb_csv_dict, easeb_csv_fields, id_is_tline=True)
write_dict_to_csv(tline_prof_csv_out, prof_csv_dict, prof_csv_fields, id_is_tline=True)
write_dict_to_csv(tline_relim_csv_out, relim_csv_dict, relim_csv_fields, id_is_tline=True)
