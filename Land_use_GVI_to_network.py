# -*- coding: utf-8 -*-

"""
Usage:
    This script is an extension for the script Street_view_GVI_to_network.
    It is intended for complementing the Google Street View (GSV) based green view index (GVI) with land use
    based green view index. The script identifies all the segments without GVI index and calculates the land use
    based GVI for these segments

     NOTE: THE REQUIRED INPUT DATASETS NEED TO BE STORED IN POSTGRESQL DATABASE WITH POSTGIS EXTENSION ENABLED

Created
    22.10.2019

Author: 
    Elias Willberg
"""

import pandas as pd # Written with version 0.23.4
import geopandas as gpd # Written with version 0.4.0
import os
from pyproj import CRS # Written with version 1.9.5.1
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# Open the database connection
engine = create_engine('postgresql://*USERNAME:*PASSWORD!@*SERVER ADDRESS:*PORT/*DATABASE', echo=False) #add your connection parameters
con = engine.raw_connection()
cursor = con.cursor()
con.autocommit = True

def TreeCover_GVI_to_segments(roadNetworkTable, treeLayerTable, roadNetworkSchema, treelayerSchame,
                              roadLayerIDfield, treeLayerIDfield):
    """
    Input:  1) Land use layer containing over 2m tree cover as polygons.
                Similar to this dataset: https://hri.fi/data/en_GB/dataset/paakaupunkiseudun-maanpeiteaineisto
            2) Street network layer WITH Google Street View based green view index

            NOTE: THE REQUIRED INPUT DATASETS NEED TO BE STORED IN POSTGRESQL DATABASE WITH POSTGIS EXTENSION ENABLED

    Output: 1) Street network layer with full GVI index (GSV + land use) attached for all the segments
    """

    # Align projections by reprojecting the tree cover table to the coordinate system of the road network table
    cursor.execute("SELECT Find_SRID('" + str(roadNetworkSchema) + "','" + str(roadNetworkTable) + "', 'geom');")
    epsg = [x[0] for x in cursor.fetchall()]
    epsg = epsg[0]

    cursor.execute("ALTER TABLE " + str(treelayerSchame) + "." + str(treeLayerTable) + " "
                    "ALTER COLUMN geom "
                    "TYPE Geometry(MultiPolygon, " + str(epsg) + ") "
                    "USING ST_Transform(geom, " + str(epsg) + ");")

    # Fix invalid geometries´in the tree cover layer
    cursor.execute("UPDATE " + str(treelayerSchame) + "." + str(treeLayerTable) + " "
                   "SET geom=ST_Multi(ST_CollectionExtract(ST_MakeValid(geom), 3)) "
                   "WHERE NOT ST_IsValid(geom);")

    # Create a table of the street network with 30m buffer zone around each street segment as tbe geometry
    cursor.execute("CREATE TABLE " + str(roadNetworkSchema) + ".streetnetwork_buffer_test AS "
                   "AS SELECT " + str(roadLayerIDfield) + ", st_buffer(geom, 30):: geometry(geometry, " + str(epsg) + ") AS geom "
                    "FROM " + str(roadNetworkSchema) + "." + str(roadNetworkTable) + ";")

    # Run the intersection between the street buffer polygons and the tree cover polygons. The function also
    # calculates the share of each tree cover polygon area of the total buffer polygon area in m2
    cursor.execute("CREATE TABLE " + str(roadNetworkSchema) + ".over2m_trees_within_streetnetwork_buffer AS "
                   "SELECT tr." + str(treeLayerIDfield) + " AS trees, bn." + str(roadLayerIDfield) + " AS buffer, ST_Area(bn.geom) as buffer_area, "
                   "ROUND((ST_Area(ST_Intersection(tr.geom, bn.geom)))::numeric,2) AS area_piece, "
                   "ROUND((ST_Area(ST_Intersection(tr.geom, bn.geom)) / ST_Area(bn.geom) * 100)::numeric,1) AS pct_in "
                   "FROM " + str(roadNetworkSchema) + "." + str(roadNetworkTable) + " bn, " + str(treelayerSchame) + "." + str(treeLayerTable) + " tr "
                   "WHERE ST_Intersects(tr.geom, bn.geom) "
                   "ORDER BY bn." + str(roadLayerIDfield) + ", tr." + str(treeLayerIDfield) + ", pct_in DESC;")

    # Group the tree cover polygons together and calculate the combined area and the share of tree cover of the total
    # buffer poltgon area. The share will be used as the green index for each segment
    cursor.execute("SELECT buffer, buffer_area, sum(area_piece) as intersect_area, sum(pct_in) as prct "
                   "FROM "+ str(roadNetworkSchema) + ".over2m_trees_within_streetnetwork_buffer"
                   "GROUP BY buffer, buffer_area;")

    # Create a new table where the result table of tree cover based green index is joined to street network layer
    cursor.execute("CREATE TABLE " + str(roadNetworkSchema) + ".bikenetwork_with_full_gsv_landUse_green_index AS "
                   "SELECT st.*, subquery.* "
                   "FROM " + str(roadNetworkSchema) + "." + str(roadNetworkTable) + " AS st "
                   "INNER JOIN (SELECT buffer, buffer_area, sum(area_piece) as intersect_area, sum(pct_in) as prct "
                   "FROM " + str(roadNetworkSchema) + ".over2m_trees_within_streetnetwork_buffer "
                   "GROUP BY buffer, buffer_area) as subquery "
                   "ON st." + + treeLayerIDfield + "= subquery.buffer;")

    con.commit()
    con.close()



# Test run
TreeCover_GVI_to_segments("bikenetwork_with_gsv_green_index", "maanpeite_puusto_yli_2m_2018", "bss_green_index",
                          "bss_green_index", "fid_1", "id")






