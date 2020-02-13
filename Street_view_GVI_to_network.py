# -*- coding: utf-8 -*-

"""
Usage:
    This script is intended for attaching the Google Street View (GSV) based green view index (GVI)
    from points to road segments. To use this script, you a point layer containing GVI index for each point
    as well a street network layer covering the extent of the point layer.

Created
    20.10.2019

Author:
    Elias Willberg
"""

import pandas as pd # Written with version 0.23.4
import geopandas as gpd # Written with version 0.4.0
import os
from pyproj import CRS # Written with version 1.9.5.1
import matplotlib.pyplot as plt

# Set working directory
os.chdir("") # filepath here

# Data paths
roadNetworkPath = "" # filepath here
GVIpointsPath = "" # filepath here

# Read data
roadNetworkData = gpd.read_file(roadNetworkPath)
GVIpointsData = gpd.read_file(GVIpointsPath)

def GVI_to_segments(roadNetwork, GVIpoints, roadID_field, outName):

    """
    Input:  1) roadNetwork = Point layer containing GVI index
            2) GVIpoints = Street network layer covering the extent of the GVI point layer
            3) roadID_field = The name of the column containing unique IDs for road segments
            4) outName = Output file name

    Output: 1) Street network layer with the GVI index attached for all the segments within 30m from GVI points
    """

    # Check that projections of the input dataframes match and reproject if necessary
    if GVIpoints.crs != roadNetwork.crs:
        epsg = CRS(roadNetwork.crs).to_epsg()
        GVIpoints = GVIpoints.to_crs(epsg=epsg)

    # Create a 30m buffer around each GVI point
    GVIpointsBufferDF = GVIpoints.copy()
    GVIpointsBufferDF["geometry"] = GVIpointsBufferDF.geometry.buffer(30)

    # Identify the centroid of each street segment
    RoadNetworkCentroidsDF = roadNetwork.copy()
    RoadNetworkCentroidsDF['geometry'] = roadNetwork.geometry.centroid

    # Make a spatial join to identify all the road centroids that intersect with GVI point buffers
    pointJoin = gpd.sjoin(GVIpointsBufferDF, RoadNetworkCentroidsDF, how='inner', op='intersects')

    # Dissolve the joined layer based on road segment ID using mean aggfunc. This creates a mean GVI
    # for each road segment. Reduce then the 'dissolve' dataframe to contain only the necessary columns
    dissolve = pointJoin.dissolve(by=roadID_field, aggfunc='mean').reset_index()
    dissolve = dissolve[[roadID_field, "Gvi_Mean"]]

    # Join the GVI index from the 'dissolve' dataframe back to the original 'roadNetwork' dataframe
    roadNetwork = pd.merge(roadNetwork, dissolve, how = "left", left_on=roadID_field, right_on=dissolve[roadID_field])

    # Rename the GVI column
    roadNetwork.rename(columns={"Gvi_Mean" : "GSV_GVI"})

    # Examine the result as map
    my_map = roadNetworkDF.plot(column="Gvi_Mean", linewidth=0.4, cmap="RdYlGn", scheme="quantiles", k=9, alpha=0.9,
                                  legend=True)

    # Write the result out
    outfp = outName+".shp"
    roadNetwork.to_file(outfp)

    # Return the result
    return roadNetworkDF

    print("Operation successfully finished")


