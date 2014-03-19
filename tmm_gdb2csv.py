#!/usr/bin/env python
'''
    tmm_gdb2csv.py
    Author: npeterson
    Revised: 3/19/2014
    ---------------------------------------------------------------------------
    This script will use the extra attribute tables in TMM_GIS.gdb to create
    updated versions of the batchin CSVs used to construct the transit network
    for the Transit Modernization Model. A new network can then be constructed
    using the altered batchin files to model the transit improvements.

'''
import os
import sys
import arcpy
import TMM

# Set parameters:
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


# Load extra attribute tables into dictionaries:
tline_dict = TMM.make_attribute_dict(tline_table, 'TLINE_ID', TMM.tline_fields)
node_dict = TMM.make_attribute_dict(node_table, 'NODE_ID', TMM.node_fields)
