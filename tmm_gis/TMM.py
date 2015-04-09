#!/usr/bin/env python
'''
    TMM.py
    Author: npeterson
    Revised: 4/9/2015
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
gdb_dir = os.path.dirname(os.path.dirname(prog_dir))
input_dir = os.path.join(gdb_dir, 'input')
output_dir = os.path.join(gdb_dir, 'output')
gdb_name = 'TMM_GIS'
gdb = os.path.join(gdb_dir, '{0}.gdb'.format(gdb_name))
proj = os.path.join(prog_dir, 'TMM_NAD27.prj')


# -----------------------------------------------------------------------------
#  2. MISCELLANEOUS PARAMETERS
# -----------------------------------------------------------------------------
node_id_int_field = 'ID_int'  # Emme exports node "ID" values as floats. Need ints for queries.

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
    'IMP_WARMING',
    'ADD_PARKING',
)

tline_fields = (
    'ADD_STAND_CAP',
    'ADD_WIFI',
    'IMP_SEATS',
    'LOWER_FLOOR',
    'NEW_VEHICLES',
    'IMP_RELIABILITY',
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


def ensure_dir(directory):
    ''' Checks for the existence of a directory, creating it if it doesn't
        exist yet. '''
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory


def make_attribute_dict(fc, key_field, attr_list=['*']):
    ''' Create a dictionary of feature class/table attributes, using OID as the
        key. Default of ['*'] for attr_list (instead of actual attribute names)
        will create a dictionary of all attributes.
        - NOTE 1: when key_field is the OID field, the OID attribute name can
          be fetched by MHN.determine_OID_fieldname(fc).
        - NOTE 2: using attr_list=[] will essentially build a list of unique
          key_field values. '''
    attr_dict = {}
    fc_field_objects = arcpy.ListFields(fc)
    fc_fields = [field.name for field in fc_field_objects if field.type != 'Geometry']
    if attr_list == ['*']:
        valid_fields = fc_fields
    else:
        valid_fields = [field for field in attr_list if field in fc_fields]
    # Ensure that key_field is always the first field in the field list
    cursor_fields = [key_field] + list(set(valid_fields) - set([key_field]))
    with arcpy.da.SearchCursor(fc, cursor_fields) as cursor:
        for row in cursor:
            attr_dict[row[0]] = dict(zip(cursor.fields, row))
    return attr_dict
