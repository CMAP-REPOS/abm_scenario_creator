#!/usr/bin/env python
'''
    TMM.py
    Author: npeterson
    Revised: 1/29/2014
    ---------------------------------------------------------------------------
    This module stores information used by other TMM scripts.

'''
import os
import sys
import arcpy

prog_dir = os.path.dirname(os.path.realpath(__file__))
gdb_dir = prog_dir.rsplit(os.sep, 1)[0]
gdb_name = 'TMM_GIS'
gdb = gdb_dir + os.sep + gdb_name + '.gdb'
proj = prog_dir + os.sep + 'TMM_NAD27.prj'

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
