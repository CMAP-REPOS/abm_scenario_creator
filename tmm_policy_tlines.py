#!/usr/bin/env python
'''
    tmm_policy_tlines.py
    Author: npeterson
    Revised: 5/23/2014
    ---------------------------------------------------------------------------
    This script will update the rows in the extra_attr_tlines table, which
    stores updated policies for all bus and train routes, for any that are
    currently selected. Must be run from within ArcMap.

'''
import os
import sys
import arcpy
import TMM

# Set parameters:
tlines_lyr = arcpy.GetParameterAsText(0)
policy_values = [arcpy.GetParameter(i+1) for i in xrange(len(TMM.tline_fields))]


# Verify some features are selected, otherwise fail:
if not TMM.check_selection(tlines_lyr):
    TMM.die('You must select at least one feature from "{0}" before continuing. (If you want the policy changes to be regionwide, please select all features.)'.format(tlines_lyr))


# Iterate through extra_attr_tlines table, updating rows for selected features:
selected_tlines = [row[0] for row in arcpy.da.SearchCursor(tlines_lyr, ['ID'])]

tline_table = os.path.join(TMM.gdb, 'extra_attr_tlines')
with arcpy.da.UpdateCursor(tline_table, TMM.tline_fields, ''' "TLINE_ID" IN ('{0}') '''.format("','".join(selected_tlines))) as cursor:
    for row in cursor:
        for i in xrange(len(row)):
            row[i] = policy_values[i] if policy_values[i] > 0 else row[i]
        cursor.updateRow(row)
