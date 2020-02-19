# -*- coding: utf-8 -*-

"""
Usage:
    This script is an extension for the script Street_view_GVI_to_network.
    It is intended for complementing the Google Street View (GSV) based green view index (GVI) with land use
    based green view index. The script identifies all the segments without GVI index and calculates the land use
    based GVI for these segments

     NOTE: MAJORITY OF THE SCRIPT IS DESIGNED WITH POSTGRESQL&PYTHON INTEGRATION. THE REQUIRED INPUT DATA SETS NEED
     TO BE STORED IN POSTGRESQL DATABASE WITH A POSTGIS EXTENSION ENABLED

Created
    22.10.2019

Author: 
    Elias Willberg
"""

from sqlalchemy import create_engine

# Open the database connection
engine = create_engine('postgresql://*USERNAME:*PASSWORD!@*SERVER ADDRESS:*PORT/*DATABASE', echo=False) #add your connection parameters
engine = create_engine('postgresql://elwi:DigiGeoElias!@dgl-data.geography.helsinki.fi:5432/cycling', echo=False)
con = engine.raw_connection()
cursor = con.cursor()

"""
                    FOLLOWING INPUTS ARE REQUIRED BY THE FUNCTIONS BELOW

  NOTE: THE REQUIRED INPUT DATASETS NEED TO BE STORED IN POSTGRESQL DATABASE WITH POSTGIS EXTENSION ENABLED
            
  Input:  1) roadNetworkTable = Street network table WITH Google Street View based green view index
          2) treeLayerTable = Land use table containing over 2m tree cover as polygons
              Similar to this dataset: https://hri.fi/data/en_GB/dataset/paakaupunkiseudun-maanpeiteaineisto
          3) roadNetworkSchema = Schema name for roadNetworkTable
          4) treelayerSchame = Schema name for treeLayerTable
          5) roadLayerIDfield = The name of the column containing unique IDs for road segments
          6) treeLayerIDfield = The name of the column containing unique IDs for tree polygons

  Output: 1) Street network layer with full GVI index (GSV + land use) attached for all the segments
  """

def AlignProjections(roadNetworkTable, treeLayerTable, roadNetworkSchema, treelayerSchame):

    con = engine.raw_connection()
    cursor = con.cursor()

    # Align projections by reprojecting the tree cover table to the coordinate system of the road network table
    cursor.execute("SELECT Find_SRID('" + str(roadNetworkSchema) + "','" + str(roadNetworkTable) + "', 'geom');")
    epsg = [x[0] for x in cursor.fetchall()]
    epsg = epsg[0]

    cursor.execute("ALTER TABLE " + str(treelayerSchame) + "." + str(treeLayerTable) + " "
                    "ALTER COLUMN geom "
                    "TYPE Geometry(MultiPolygon, " + str(epsg) + ") "
                    "USING ST_Transform(geom, " + str(epsg) + ");")

    con.commit()
    con.close()

def fixGeometries(treeLayerTable, treelayerSchame):

    con = engine.raw_connection()
    cursor = con.cursor()

    # Fix invalid geometries´in the tree cover layer
    cursor.execute("UPDATE " + str(treelayerSchame) + "." + str(treeLayerTable) + " "
                   "SET geom=ST_Multi(ST_CollectionExtract(ST_MakeValid(geom), 3)) "
                   "WHERE NOT ST_IsValid(geom);")
    con.commit()

def bufferRoads(roadNetworkTable, roadNetworkSchema, roadLayerIDfield):

    con = engine.raw_connection()
    cursor = con.cursor()

    cursor.execute("SELECT Find_SRID('" + str(roadNetworkSchema) + "','" + str(roadNetworkTable) + "', 'geom');")
    epsg = [x[0] for x in cursor.fetchall()]
    epsg = epsg[0]

    # Create a table of the street network with 30m buffer zone where the buffer geometry is tbe geometry column
    cursor.execute("CREATE TABLE " + str(roadNetworkSchema) + ".streetnetwork_buffer AS "
                   "SELECT " + str(roadLayerIDfield) + ", st_buffer(geom, 30):: geometry(geometry, " + str(epsg) + ") AS geom "
                    "FROM " + str(roadNetworkSchema) + "." + str(roadNetworkTable) + ";")

    con.commit()
    con.close()

def streetTreeIntersection(treeLayerTable, roadNetworkSchema, treelayerSchame,
                                  roadLayerIDfield, treeLayerIDfield):
    con = engine.raw_connection()
    cursor = con.cursor()

    # Run the intersection between the street buffer polygons and the tree cover polygons. The function also
    # calculates the share of each tree cover polygon area of the total buffer polygon area in m2
    cursor.execute("CREATE TABLE " + str(roadNetworkSchema) + ".over2m_trees_within_streetnetwork_buffer AS "
                   "SELECT tr." + str(treeLayerIDfield) + " AS trees, bn." + str(roadLayerIDfield) + " AS buffer, ST_Area(bn.geom) as buf_area, "
                   "ROUND((ST_Area(ST_Intersection(tr.geom, bn.geom)))::numeric,2) AS area_piece, "
                   "ROUND((ST_Area(ST_Intersection(tr.geom, bn.geom)) / ST_Area(bn.geom) * 100)::numeric,1) AS pct_in "
                   "FROM " + str(roadNetworkSchema) + ".streetnetwork_buffer bn," + str(treelayerSchame) + "." + str(treeLayerTable) + " tr "
                   "WHERE ST_Intersects(tr.geom, bn.geom) "
                   "ORDER BY bn." + str(roadLayerIDfield) + ", tr." + str(treeLayerIDfield) + ", pct_in DESC;")

    con.commit()
    con.close()

def calculateTreeCoverShare(roadNetworkSchema):

    con = engine.raw_connection()
    cursor = con.cursor()

    # Group the tree cover polygons together and calculate the combined area and the share of tree cover of the total
    # buffer poltgon area. The share is the green index for each segment
    cursor.execute("SELECT buffer, buf_area, sum(area_piece) as lu_area, sum(pct_in) as lu_gvi "
                   "FROM "+ str(roadNetworkSchema) + ".over2m_trees_within_streetnetwork_buffer "
                   "GROUP BY buffer, buf_area;")

    con.commit()
    con.close()

def createFullGreenIndexTable(roadNetworkTable, roadNetworkSchema, roadLayerIDfield):

    con = engine.raw_connection()
    cursor = con.cursor()

    # Create a new table where the result table of tree cover based green index is joined to street network layer
    cursor.execute("CREATE TABLE " + str(roadNetworkSchema) + ".bikenetwork_with_full_gsv_landUse_green_index AS "
                   "SELECT st.*, subquery.* "
                   "FROM " + str(roadNetworkSchema) + "." + str(roadNetworkTable) + " AS st "
                   "INNER JOIN (SELECT buffer, buf_area, sum(area_piece) as lu_area, sum(pct_in) as lu_gvi "
                   "FROM " + str(roadNetworkSchema) + ".over2m_trees_within_streetnetwork_buffer "
                   "GROUP BY buffer, buf_area) as subquery "
                   "ON st." + roadLayerIDfield + "= subquery.buffer;")
    con.commit()
    con.close()

def updateFullGreenIndexTable(roadNetworkSchema):

    con = engine.raw_connection()
    cursor = con.cursor()

    # Create a new column to the table for the combined index
    cursor.execute("ALTER TABLE " + str(roadNetworkSchema) + ".bikenetwork_with_full_gsv_landUse_green_index "
                   "ADD comb_gvi numeric;")

    # Create a new column to the table to indicate the source of the combined index
    cursor.execute("ALTER TABLE " + str(roadNetworkSchema) + ".bikenetwork_with_full_gsv_landUse_green_index "
                    "ADD gvi_source text;")

    # Fill the combined index column. If GSV based GVI is available (not -1) use that else use land use based index
    cursor.execute("UPDATE " + str(roadNetworkSchema) + ".bikenetwork_with_full_gsv_landUse_green_index "
                   "SET comb_gvi = case when (gsv_gvi = -1) then lu_gvi else gsv_gvi end;")

    # Fill the gvi_source column based on which value GVI is based.
    cursor.execute("UPDATE " + str(roadNetworkSchema) + ".bikenetwork_with_full_gsv_landUse_green_index "
                   "SET gvi_source = case when (gsv_gvi = -1) then 'land_use' else 'gsv' end;")

    con.commit()
    con.close()


# Run the functions  (OBS! Change the parameters to your own)
AlignProjections("bikenetwork_with_gsv_green_index", "maanpeite_puusto_yli_2m_2018", "bss_green_index","bss_green_index")
fixGeometries("maanpeite_puusto_yli_2m_2018", "bss_green_index")
bufferRoads("bikenetwork_with_gsv_green_index", "bss_green_index", "fid_1")
streetTreeIntersection("maanpeite_puusto_yli_2m_2018", "bss_green_index","bss_green_index", "fid_1", "id")
calculateTreeCoverShare("bss_green_index")
createFullGreenIndexTable("bikenetwork_with_gsv_green_index", "bss_green_index", "fid_1")
updateFullGreenIndexTable("bss_green_index")








