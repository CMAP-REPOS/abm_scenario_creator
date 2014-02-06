#!/usr/bin/env python
'''
    tmm_shp2gdb.py
    Author: npeterson
    Revised: 2/6/2014
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
import TMM

# Set parameters:
shp_root_dir = arcpy.GetParameterAsText(0)  # 'C:\\WorkSpace\\TransitModernizationModel\\TMM_Test\\Media'


# Create geodatabase:
arcpy.AddMessage('\nCreating geodatabase {0}...\n'.format(TMM.gdb))
TMM.delete_if_exists(TMM.gdb)
arcpy.CreateFileGDB_management(TMM.gdb_dir, TMM.gdb_name)


# Create TOD-specific FDs and FCs from shapefiles and identify unique node/tline IDs:
unique_nodes = set()
unique_tlines = set()

for tod in (1, 2, 3, 4, 5, 6, 7, 8):
    arcpy.AddMessage('TOD {0}:'.format(tod))
    shp_dir = os.path.join(shp_root_dir, 'Scenario_10{0}'.format(tod))
    tod_fd_name = 'tod_{0}'.format(tod)
    tod_fd = os.path.join(TMM.gdb, tod_fd_name)
    day_fd_name = 'tod_all'
    day_fd = os.path.join(TMM.gdb, day_fd_name)

    arcpy.AddMessage('-- Creating feature dataset...')
    arcpy.CreateFeatureDataset_management(TMM.gdb, tod_fd_name, TMM.proj)
    if not arcpy.Exists(day_fd):
        arcpy.CreateFeatureDataset_management(TMM.gdb, day_fd_name, TMM.proj)

    arcpy.AddMessage('-- Creating feature classes from shapefiles...')
    for dirpath, dirnames, filenames in arcpy.da.Walk(shp_dir):
        for filename in filenames:
            if filename.endswith('.shp'):
                shp_file = os.path.join(dirpath, filename)
                gdb_fc = os.path.join(tod_fd, '{0}_{1}'.format(filename[:-4], tod))
                arcpy.DefineProjection_management(shp_file, TMM.proj)
                arcpy.CopyFeatures_management(shp_file, gdb_fc)

    node_fc = os.path.join(tod_fd, 'emme_nodes_{0}'.format(tod))
    tline_fc = os.path.join(tod_fd, 'emme_tlines_{0}'.format(tod))
    tseg_fc = os.path.join(tod_fd, 'emme_tsegs_{0}'.format(tod))

    # Create an integer version of the nodes 'ID' field (which is FLOAT):
    arcpy.AddField_management(node_fc, TMM.node_id_field, 'LONG')
    arcpy.CalculateField_management(node_fc, TMM.node_id_field, 'int(round(!ID!))', 'PYTHON_9.3')

    # Append unique tlines to all-day fc:
    arcpy.AddMessage('-- Identifying unique tlines...')
    day_tline_fc = os.path.join(day_fd, 'emme_tlines_all')
    day_node_fc = os.path.join(day_fd, 'emme_nodes_all')

    new_tlines_lyr = 'new_tlines_lyr'
    if not arcpy.Exists(day_tline_fc):
        arcpy.MakeFeatureLayer_management(tline_fc, new_tlines_lyr)
        arcpy.CopyFeatures_management(new_tlines_lyr, day_tline_fc)
    else:
        new_tlines_query = ''' "ID" NOT IN ('{0}') '''.format("','".join((tline_id for tline_id in unique_tlines)))
        arcpy.MakeFeatureLayer_management(tline_fc, new_tlines_lyr, new_tlines_query)
        arcpy.Append_management([new_tlines_lyr], day_tline_fc)

    # Append new unique nodes from TOD's itineraries to all-day fc:
    arcpy.AddMessage('-- Identifying unique nodes...\n')
    new_tlines = (row[0] for row in arcpy.da.SearchCursor(new_tlines_lyr, ['ID']))
    new_tsegs_lyr = 'new_tsegs_lyr'
    new_tsegs_query = ''' "LINE_ID" IN ('{0}') '''.format("','".join((tline_id for tline_id in new_tlines)))
    arcpy.MakeFeatureLayer_management(tseg_fc, new_tsegs_lyr, new_tsegs_query)
    new_tline_nodes = set()
    with arcpy.da.SearchCursor(new_tsegs_lyr, ['INODE', 'JNODE']) as cursor:
        for row in cursor:
            new_tline_nodes.update(row)
    new_nodes = new_tline_nodes.difference(unique_nodes)

    if not arcpy.Exists(day_node_fc):
        arcpy.CreateFeatureclass_management(os.path.split(day_node_fc)[0], os.path.split(day_node_fc)[1], 'POINT', node_fc)
    new_nodes_lyr = 'new_nodes_lyr'
    new_nodes_query = ''' "{0}" IN ({1}) '''.format(TMM.node_id_field, ','.join((str(node_id) for node_id in new_nodes)))
    arcpy.MakeFeatureLayer_management(node_fc, new_nodes_lyr, new_nodes_query)
    arcpy.Append_management([new_nodes_lyr], day_node_fc)

    arcpy.Delete_management(new_tlines_lyr)
    arcpy.Delete_management(new_tsegs_lyr)
    arcpy.Delete_management(new_nodes_lyr)

    # Update global list of unique IDs:
    unique_tlines.update(new_tlines)
    unique_nodes.update(new_nodes)


# Create extra attribute tables:
arcpy.AddMessage('Creating tline extra attribute table...')
tline_table_name = 'extra_attr_tlines'
tline_table = os.path.join(TMM.gdb, tline_table_name)
arcpy.CreateTable_management(TMM.gdb, tline_table_name)

arcpy.AddMessage('Creating node extra attribute table...\n')
node_table_name = 'extra_attr_nodes'
node_table = os.path.join(TMM.gdb, node_table_name)
arcpy.CreateTable_management(TMM.gdb, node_table_name)


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
arcpy.AddMessage('Adding extra attribute fields to node table...')
for field_name in TMM.node_fields:
    arcpy.AddField_management(node_table, field_name, 'SHORT')
    arcpy.CalculateField_management(node_table, field_name, '0', 'PYTHON')

arcpy.AddMessage('Adding extra attribute fields to tline table...\n')
for field_name in TMM.tline_fields:
    arcpy.AddField_management(tline_table, field_name, 'SHORT')
    arcpy.CalculateField_management(tline_table, field_name, '0', 'PYTHON')

arcpy.AddMessage('All done!\n')
