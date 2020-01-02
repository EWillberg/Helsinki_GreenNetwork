# -*- coding: utf-8 -*-

"""
Usage:
    This script is intended for attaching the Google Street View (GSV) based green view index (GVI)
    from points to road segments.

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
roadNetworkDF = gpd.read_file(roadNetworkPath)
GVIpointsDF = gpd.read_file(GVIpointsPath)

def GVI_to_segments(roadNetwork, GVIpoints, roadID_field, outName):

    """
    Input:  1) Point layer containing GVI index
            2) Street network layer covering the extent of the GVI point layer

    Output: 2) Street network layer with the GVI index attached for all the segments within 30m from GVI points
    """

    # Check that projections of the input dataframes match and reproject if necessary
    if GVIpoints.crs != roadNetwork.crs:
        epsg = CRS(streetNetworkDF.crs).to_epsg()
        GVIpoints = GVIpoints.to_crs(epsg=epsg)

    # Create a 30m buffer around streetNetwork segments
    GVIpointsBufferDF = GVIpoints.copy()
    GVIpointsBufferDF["geometry"] = GVIpointsBufferDF.geometry.buffer(30)

    # Identify the centroid of each street segment
    RoadNetworkCentroidsDF = roadNetwork.copy()
    RoadNetworkCentroidsDF['geometry'] = roadNetwork.geometry.centroid

    # Make a spatial join to identify all the road segment buffers that intersect with GVI points
    pointJoin = gpd.sjoin(GVIpointsBufferDF, RoadNetworkCentroidsDF, how='inner', op='intersects')

    # Dissolve the joined layer based on road segment ID using mean aggfunc. This creates a mean GVI
    # for each road segment. Reduce then the "dissolve" dataframe only to contain necessary columns
    dissolve = pointJoin.dissolve(by=roadID_field, aggfunc='mean').reset_index()
    dissolve = dissolve[[roadID_field, "Gvi_Mean"]]

    # Join the GVI index from the "dissolve" dataframe back to the "roadNetwork" dataframe
    roadNetwork = pd.merge(roadNetwork, dissolve, how = "left", left_on=roadID_field, right_on=dissolve[roadID_field])

    # Examine the result
    my_map = roadNetwork.plot(column="Gvi_Mean", linewidth=0.4, cmap="RdYlGn", scheme="quantiles", k=9, alpha=0.9,
                                  legend=True)

    # Write the result
    outfp = outName+".shp"
    roadNetwork.to_file(outfp)

    # Return the result
    return roadNetwork

    print("Operation successfully finished")


