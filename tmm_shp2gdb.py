#!/usr/bin/env python
'''
    tmm_shp2gdb.py
    Author: npeterson
    Revised: 1/28/2014
    ---------------------------------------------------------------------------
    This script will convert a set of shapefiles generated from all 8 TODs in
    an Emme network (via Emme's "Export Network As Shapefile" Modeller tool)
    into feature classes in a new geodatabase.

    This script will also iterate through TMM nodes and transit lines,
    creating tables to store specific policy-based extra attributes for each
    unique feature.

'''
import os
import sys
import arcpy

shp_root_dir = arcpy.GetParameterAsText(0)              # 'C:/WorkSpace/TransitModernizationModel/TMM_Test/Media'
prog_dir = os.path.dirname(os.path.realpath(__file__))  # 'C:/WorkSpace/TransitModernizationModel/TMM_GIS/TMM_Programs'
gdb_dir = prog_dir.rsplit(os.sep, 1)[0]                 # 'C:/WorkSpace/TransitModernizationModel/TMM_GIS'
gdb_name = 'TMM_GIS'
gdb = gdb_dir + '/' + gdb_name + '.gdb'
tmm_proj = prog_dir + '/TMM_NAD27.prj'

unique_nodes = set()
unique_tlines = set()


# Create geodatabase
arcpy.AddMessage('\nCreating geodatabase {0}...\n'.format(gdb))
if arcpy.Exists(gdb):
    arcpy.Delete_management(gdb)
arcpy.CreateFileGDB_management (gdb_dir, gdb_name)


# Create TOD-specific FDs and FCs from shapefiles and identify unique node/tline IDs:
for tod in (1, 2, 3, 4, 5, 6, 7, 8):
    arcpy.AddMessage('TOD {0}:'.format(tod))
    shp_dir = shp_root_dir + '/Scenario_10{0}'.format(tod)
    tod_fd_name = 'tod_{0}'.format(tod)
    tod_fd = gdb + '/' + tod_fd_name
    day_fd_name = 'tod_all'
    day_fd = gdb + '/' + day_fd_name

    arcpy.AddMessage('-- Creating feature dataset...')
    arcpy.CreateFeatureDataset_management(gdb, tod_fd_name, tmm_proj)
    if not arcpy.Exists(day_fd):
        arcpy.CreateFeatureDataset_management(gdb, day_fd_name, tmm_proj)

    arcpy.AddMessage('-- Creating feature classes from shapefiles...')
    for dirpath, dirnames, filenames in arcpy.da.Walk(shp_dir):
        for filename in filenames:
            if filename.endswith('.shp'):
                shp_file = os.path.join(dirpath, filename)
                gdb_fc = tod_fd + '/{0}_{1}'.format(filename[:-4], tod)
                arcpy.DefineProjection_management(shp_file, tmm_proj)
                arcpy.CopyFeatures_management(shp_file, gdb_fc)

    node_fc = tod_fd + '/emme_nodes_{0}'.format(tod)
    tline_fc = tod_fd + '/emme_tlines_{0}'.format(tod)

    # Create an integer version of the nodes 'ID' field (which is FLOAT):
    node_id_field = 'ID_int'
    arcpy.AddField_management(node_fc, node_id_field, 'LONG')
    arcpy.CalculateField_management(node_fc, node_id_field, 'int(round(!ID!))', 'PYTHON_9.3')

    # Append unique nodes/tlines to all-day feature classes
    arcpy.AddMessage('-- Identifying unique nodes/tlines...\n')
    day_node_fc = day_fd + '/emme_nodes_all'
    day_tline_fc = day_fd + '/emme_tlines_all'

    if not arcpy.Exists(day_node_fc):
        arcpy.CopyFeatures_management(node_fc, day_node_fc)
    else:
        new_nodes_lyr = 'new_nodes_lyr'
        new_nodes_query = ''' "{0}" NOT IN ({1}) '''.format(node_id_field, ','.join((str(id) for id in unique_nodes)))
        arcpy.MakeFeatureLayer_management(node_fc, new_nodes_lyr, new_nodes_query)
        arcpy.Append_management([new_nodes_lyr], day_node_fc)
        arcpy.Delete_management(new_nodes_lyr)

    if not arcpy.Exists(day_tline_fc):
        arcpy.CopyFeatures_management(tline_fc, day_tline_fc)
    else:
        new_tlines_lyr = 'new_tlines_lyr'
        new_tlines_query = ''' "ID" NOT IN ('{0}') '''.format("','".join((id for id in unique_tlines)))
        arcpy.MakeFeatureLayer_management(tline_fc, new_tlines_lyr, new_tlines_query)
        arcpy.Append_management([new_tlines_lyr], day_tline_fc)
        arcpy.Delete_management(new_tlines_lyr)

    # Iterate through TOD FCs and identify all unique IDs:
    unique_nodes.update((row[0] for row in arcpy.da.SearchCursor(node_fc, [node_id_field])))
    unique_tlines.update((row[0] for row in arcpy.da.SearchCursor(tline_fc, ['ID'])))


# Create extra attribute tables:
arcpy.AddMessage('Creating node extra attribute table...')
node_table_name = 'extra_attr_nodes'
node_table = gdb + '/' + node_table_name
arcpy.CreateTable_management(gdb, node_table_name)

arcpy.AddMessage('Creating tline extra attribute table...\n')
tline_table_name = 'extra_attr_tlines'
tline_table = gdb + '/' + tline_table_name
arcpy.CreateTable_management(gdb, tline_table_name)


# Populate extra attibute tables with unique IDs:
arcpy.AddMessage('Populating node table with IDs...')
arcpy.AddField_management(node_table, 'NODE_ID', 'LONG')
with arcpy.da.InsertCursor(node_table, ['NODE_ID']) as cursor:
    for node_id in sorted(list(unique_nodes)):
        cursor.insertRow([node_id])

arcpy.AddMessage('Populating tline table with IDs...\n')
arcpy.AddField_management(tline_table, 'TLINE_ID', 'TEXT', field_length=20)
with arcpy.da.InsertCursor(tline_table, ['TLINE_ID']) as cursor:
    for tline_id in sorted(list(unique_tlines)):
        cursor.insertRow([tline_id])


# Add policy fields to node/tline tables:
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

arcpy.AddMessage('Adding extra attribute fields to node table...')
for field_name in node_fields:
    arcpy.AddField_management(node_table, field_name, 'SHORT')

arcpy.AddMessage('Adding extra attribute fields to tline table...\n')
for field_name in tline_fields:
    arcpy.AddField_management(tline_table, field_name, 'SHORT')

arcpy.AddMessage('All done!\n')
