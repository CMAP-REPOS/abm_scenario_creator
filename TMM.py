#!/usr/bin/env python
'''
    TMM.py
    Author: npeterson
    Revised: 2/6/2014
    ---------------------------------------------------------------------------
    This module stores information used by other TMM scripts.

'''
import os
import sys
import arcpy
arcpy.env.OverwriteOutput = True

# -----------------------------------------------------------------------------
#  1. DIRECTORIES & FILES
# -----------------------------------------------------------------------------
prog_dir = os.path.dirname(os.path.realpath(__file__))
gdb_dir = os.path.split(prog_dir)[0]
gdb_name = 'TMM_GIS'
gdb = os.path.join(gdb_dir, '{0}.gdb'.format(gdb_name))
proj = os.path.join(prog_dir, 'TMM_NAD27.prj')


# -----------------------------------------------------------------------------
#  2. MISCELLANEOUS PARAMETERS
# -----------------------------------------------------------------------------
node_id_field = 'ID_int'

node_fields = (
    'ADD_ADA',
    'ADD_INFO',
    'ADD_PA',
    'ADD_RETAIL',
    'ADD_SEATS',
    'ADD_SEC_CAM',
    'ADD_SHELTER',
    'ADD_WALKWAY',
    'ENLARGE_AREA',
    'FACELIFT',
    'IMP_LIGHTING',
    'IMP_WARMING'
)

tline_fields = (
    'ADD_STAND_CAP',
    'ADD_WIFI',
    'IMP_SEATS',
    'LOWER_FLOOR',
    'NEW_VEHICLES'
)


# -----------------------------------------------------------------------------
#  3. METHODS
# -----------------------------------------------------------------------------
def check_selection(lyr):
    ''' Check whether specified feature layer has a selection. '''
    import arcpy
    desc = arcpy.Describe(lyr)
    selected = desc.FIDSet
    if len(selected) == 0:
        return False
    else:
        return True


def delete_if_exists(filepath):
    ''' Check if a file exists, and delete it if so. '''
    if arcpy.Exists(filepath):
        arcpy.Delete_management(filepath)
        message = filepath + ' successfully deleted.'
    else:
        message = filepath + ' does not exist.'
    return message


def die(error_message):
    ''' End processing prematurely. '''
    arcpy.AddError('\n' + error_message + '\n')
    sys.exit()
    return None
