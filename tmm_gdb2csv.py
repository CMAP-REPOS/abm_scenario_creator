#!/usr/bin/env python
'''
    tmm_gdb2csv.py
    Author: npeterson
    Revised: 3/20/2014
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

boarding_ease_csv_in = os.path.join(input_dir, 'boarding_ease_by_line.csv')
bus_node_attr_csv_in = os.path.join(input_dir, 'bus_node_extra_attributes.csv')
rail_node_attr_csv_in = os.path.join(input_dir, 'rail_node_extra_attributes.csv')

boarding_ease_csv_out = boarding_ease_csv_in.replace(input_dir, output_dir)
bus_node_attr_csv_out = bus_node_attr_csv_in.replace(input_dir, output_dir)
rail_node_attr_csv_out = rail_node_attr_csv_in.replace(input_dir, output_dir)


# -----------------------------------------------------------------------------
#  Load input tables into Python objects.
# -----------------------------------------------------------------------------
tline_dict = TMM.make_attribute_dict(tline_table, 'TLINE_ID', TMM.tline_fields)
node_dict = TMM.make_attribute_dict(node_table, 'NODE_ID', TMM.node_fields)

bus_csv_dict = {}
bus_csv_fields = []
with open(bus_node_attr_csv_in, 'r') as attr_csv:
    dict_reader = csv.DictReader(attr_csv)
    bus_csv_fields.extend(dict_reader.fieldnames)
    node_id_field = bus_csv_fields[0]
    for row_dict in dict_reader:
        node_id = int(row_dict[node_id_field])
        bus_csv_dict[node_id] = row_dict.copy()

rail_csv_dict = {}
rail_csv_fields = []
with open(rail_node_attr_csv_in, 'r') as attr_csv:
    dict_reader = csv.DictReader(attr_csv)
    rail_csv_fields.extend(dict_reader.fieldnames)
    node_id_field = rail_csv_fields[0]
    for row_dict in dict_reader:
        node_id = int(row_dict[node_id_field])
        rail_csv_dict[node_id] = row_dict.copy()


# -----------------------------------------------------------------------------
#  Define scoring functions.
# -----------------------------------------------------------------------------
def adjust_type_value(node_id, csv_dict, type_field):
    ''' Create a composite score from a subset of the GDB node table's fields,
        to provide a boost to the @bstyp/@rstyp extra attributes. '''
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
    current_type_value = int(csv_dict[node_id][type_field])
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
