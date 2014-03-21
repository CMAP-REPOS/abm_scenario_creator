#!/usr/bin/env python
'''
    tmm_gdb2csv.py
    Author: npeterson
    Revised: 3/21/2014
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
output_dir = TMM.output_dir

scen = 100  # Year 2010
tod_periods = range(1, 9)  # 1-8

tline_table = os.path.join(TMM.gdb, 'extra_attr_tlines')
node_table = os.path.join(TMM.gdb, 'extra_attr_nodes')

boarding_ease_csv_in = os.path.join(input_dir, 'boarding_ease_by_line_id.csv')
bus_node_attr_csv_in = os.path.join(input_dir, 'bus_node_extra_attributes.csv')
rail_node_attr_csv_in = os.path.join(input_dir, 'rail_node_extra_attributes.csv')

boarding_ease_csv_out = boarding_ease_csv_in.replace(input_dir, output_dir)
bus_node_attr_csv_out = bus_node_attr_csv_in.replace(input_dir, output_dir)
rail_node_attr_csv_out = rail_node_attr_csv_in.replace(input_dir, output_dir)


# -----------------------------------------------------------------------------
#  Define functions.
# -----------------------------------------------------------------------------
def adjust_type_value(node_id, node_dict, csv_dict, type_field):
    ''' Create a composite score from a subset of the GDB node table's fields,
        to provide a boost to the @bstyp/@rstyp extra attributes. '''
    # Get current type value
    current_type_value = int(csv_dict[node_id][type_field])

    # Update type values for nodes in GDB
    if node_id in node_dict:
        max_type_value = 5  # 5 = 'major terminal'

        # Set field scale factors (1 / maximum field value) & score weights
        field_fwv = {
            'ADD_ADA':      {'f': 1.0, 'w': 1.0},
            'ADD_RETAIL':   {'f': 1.0, 'w': 1.0},
            'ADD_SEATS':    {'f': 0.2, 'w': 1.0},
            'ADD_SEC_CAM':  {'f': 0.2, 'w': 1.0},
            'ADD_SHELTER':  {'f': 0.2, 'w': 1.0},
            'ADD_WALKWAY':  {'f': 1.0, 'w': 1.0},
            'ENLARGE_AREA': {'f': 0.2, 'w': 1.0},
            'FACELIFT':     {'f': 0.2, 'w': 1.0},
            'IMP_LIGHTING': {'f': 0.2, 'w': 1.0},
            'IMP_WARMING':  {'f': 0.2, 'w': 1.0},
        }

        # If node is already ADA-accessible, remove 'ADD_ADA'
        if csv_dict[node_id]['accessible'] == '1':
            node_dict[node_id]['ADD_ADA'] = 0

        # Get current field values
        for attr in field_fwv.keys():
            field_fwv[attr]['v'] = int(node_dict[node_id][attr])

        # Calculate node's improvement score (0-4)
        node_improvement = sum([field_fwv[attr]['v'] * field_fwv[attr]['f'] * field_fwv[attr]['w'] for attr in field_fwv.keys()])
        max_improvement = sum([field_fwv[attr]['w'] for attr in field_fwv.keys()])
        pct_improvement = node_improvement / max_improvement

        # Set adjusted type value
        max_adjustment = max_type_value - current_type_value
        adjustment = round(max_adjustment * pct_improvement)  # The higher the current type, the harder it is to improve
        adjusted_type_value = int(current_type_value + adjustment)
        csv_dict[node_id][type_field] = str(adjusted_type_value)
        return str(adjusted_type_value)

    # Ignore nodes not in GDB
    else:
        return str(current_type_value)


def adjust_info_value(node_id, node_dict, csv_dict, info_field):
    ''' Check existing real-time info value and update if appropriate. '''
    # Get current info value
    current_info_value = csv_dict[node_id][info_field]

    # Update info values for nodes in GDB
    if node_id in node_dict:
        if current_info_value == '2':
            adjusted_info_value = current_info_value
        else:
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

    # Ignore nodes not in GDB
    else:
        return current_info_value


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
        if id_is_tline:
            def sort_tline(key):
                ''' Order TLINE_IDs by Metra, CTA Rail, then buses. '''
                if key.startswith('m'):         # Metra (m)
                    return '1{0}'.format(key)
                elif key.startswith('c'):       # CTA Rail (c)
                    return '2{0}'.format(key)
                else:                           # Buses (b, e, l, p, q)
                    return '3{0}'.format(key)
            sorted_keys = sorted(csv_dict.keys(), key=sort_tline)
        else:
            sorted_keys = sorted(csv_dict.keys())
        for dict_id in sorted_keys:
            dict_writer.writerow(csv_dict[dict_id])
    return csv_file


# -----------------------------------------------------------------------------
#  Load input GDB tables & CSVs into dictionaries.
# -----------------------------------------------------------------------------
tline_gdb_dict = TMM.make_attribute_dict(tline_table, 'TLINE_ID', TMM.tline_fields)
node_gdb_dict = TMM.make_attribute_dict(node_table, 'NODE_ID', TMM.node_fields)

easeb_csv_dict, easeb_csv_fields = make_dict_from_csv(boarding_ease_csv_in, id_is_tline=True)
bus_csv_dict, bus_csv_fields = make_dict_from_csv(bus_node_attr_csv_in)
rail_csv_dict, rail_csv_fields = make_dict_from_csv(rail_node_attr_csv_in)


# -----------------------------------------------------------------------------
#  Update dictionary values to reflect GDB scenario.
# -----------------------------------------------------------------------------
for node_id in bus_csv_dict.keys():
    adjust_type_value(node_id, node_gdb_dict, bus_csv_dict, '@bstyp')
    adjust_info_value(node_id, node_gdb_dict, bus_csv_dict, '@bsinf')

for node_id in rail_csv_dict.keys():
    adjust_type_value(node_id, node_gdb_dict, rail_csv_dict, '@rstyp')
    adjust_info_value(node_id, node_gdb_dict, rail_csv_dict, '@rsinf')


# -----------------------------------------------------------------------------
#  Write output CSVs.
# -----------------------------------------------------------------------------
write_dict_to_csv(boarding_ease_csv_out, easeb_csv_dict, easeb_csv_fields, id_is_tline=True)
write_dict_to_csv(bus_node_attr_csv_out, bus_csv_dict, bus_csv_fields)
write_dict_to_csv(rail_node_attr_csv_out, rail_csv_dict, rail_csv_fields)
