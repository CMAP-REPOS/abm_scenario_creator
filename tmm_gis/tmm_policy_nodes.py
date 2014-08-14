#!/usr/bin/env python
'''
    tmm_policy_nodes.py
    Author: npeterson
    Revised: 5/23/2014
    ---------------------------------------------------------------------------
    This script will update the rows in the extra_attr_nodes table, which
    stores updated policies for all bus stops and train stations, for any that
    are currently selected. Must be run from within ArcMap.

'''
import os
import sys
import arcpy
import TMM

# Set parameters:
nodes_lyr = arcpy.GetParameterAsText(0)
policy_values = [arcpy.GetParameter(i+1) for i in xrange(len(TMM.node_fields))]
ignore_zeroes = arcpy.GetParameter(len(policy_values)+1)


# Verify some features are selected, otherwise fail:
if not TMM.check_selection(nodes_lyr):
    TMM.die('You must select at least one feature from "{0}" before continuing. (If you want the policy changes to be regionwide, please select all features.)'.format(nodes_lyr))


# Iterate through extra_attr_nodes table, updating rows for selected features:
selected_nodes = [str(row[0]) for row in arcpy.da.SearchCursor(nodes_lyr, [TMM.node_id_int_field])]

node_table = os.path.join(TMM.gdb, 'extra_attr_nodes')
with arcpy.da.UpdateCursor(node_table, TMM.node_fields, ''' "NODE_ID" IN ({0}) '''.format(",".join(selected_nodes))) as cursor:
    for row in cursor:
        for i in xrange(len(row)):
            if policy_values[i] > 0 or not ignore_zeroes:
                row[i] = policy_values[i]
        cursor.updateRow(row)
