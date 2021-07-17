from datetime import datetime
import os

import boto3
import fiona
import pandas as pd
import numpy as np
from pyproj import Proj, transform
from shapely.geometry import shape



class ShapefileProcessor(object):
    """Shapefiles processor class
    """
    
    def __init__(self):
        
        pass 
    
     
    def wsg_to_latlon(self, x1, x2, geo_id, inproj=32632):
        """Convert coordinates from WSG85 
        projection to latitude and longitude.

        Note: default inproj is what is used by 
        Italian Office of National Statistics (ISTAT) 
        ref: https://www.istat.it/it/files//2013/11/Descrizione-dati-Pubblicazione-2016.03.09.pdf

        Arguments:
            x1: float
            x2: float
            inproj: float

            https://gis.stackexchange.com/questions/78838/converting-projected-coordinates-to-lat-lon-using-python
        """
        
        try:
            inProj = Proj('epsg:{}'.format(inproj))
            outProj = Proj('epsg:4326')
            lat, long = transform(inProj,outProj,x1,x2)
        except:
            print("Error with section: {}".format(geo_id))
            
            

        return lat, long
     
        
        
class ItalianCensusAreas(ShapefileProcessor):
    
    
    def get_section_features(self, sezioni):
        
        lat_long_geoms, lat_long_centroids, wkts_lat_long, wkts_wsg85 = self.shapefiles_to_latlong(sezioni)
        
        # Geoms
        sezioni['geometry_ll'] = lat_long_geoms
        sezioni['centroid_ll'] = lat_long_centroids
        
        # Wkts 
        sezioni['geometry_wkt_ll'] = wkts_lat_long
        sezioni['geometry_wkt'] = wkts_wsg85
        
        
        # Extract features from properties
        for feat in sezioni.iloc[0]['properties'].keys():

            vec = [x[feat] for x in sezioni['properties']]
            assert (len(vec) == len(sezioni)) , f"{feat} is not always present"
            sezioni[feat] = vec
        
        return sezioni
    
    
    def shapefiles_to_latlong(self, sezioni):
        """Convert a shapefile in WSG84 projection 
        to latitud and longitude

        Arguments:
            sezioni: Pandas DF with a column "geometry"

        Return: 
            lat_long_geoms:list of shapefiles
            lat_long_centroids: list of tuples

        """

        lat_long_geoms = []
        lat_long_centroids = []
        wkts_lat_long = []
        wkts_wsg85 = []

        for i in sezioni.itertuples():

            if i.Index % 1000 == 0:
                print(i.Index)
                
            try:

                geom = i.geometry

                if i.geometry['type'] == 'Polygon':

                    coords = geom['coordinates'][0]

                    # Convert WSG84 proj to lat-long
                    wsg_x0 = [x[0] for x in coords]
                    wsg_x1 = [x[1] for x in coords]
                    lat, long = self.wsg_to_latlon(wsg_x0, wsg_x1, i.properties)

                    lat_centroid = np.mean(lat)
                    long_centroid = np.mean(long)

                    # Create new set of coordinates
                    new_coords = list(zip(long, lat))

                    # Create New geometry based on latitude longitude
                    new_geom = geom.copy()
                    new_geom['coordinates'] = [new_coords]
                    lat_long_geoms.append(new_geom)
                    lat_long_centroids.append((long_centroid, lat_centroid))

                    # Get well know texts 
                    sh_geom = shape(geom)
                    sh_new_geom = shape(new_geom)
                    wkts_wsg85.append(sh_geom.wkt)
                    wkts_lat_long.append(sh_new_geom.wkt)

                elif i.geometry['type'] == 'MultiPolygon':

                    coords = geom['coordinates'][0][0]

                    # Convert WSG84 proj to lat-long
                    wsg_x0 = [x[0] for x in coords]
                    wsg_x1 = [x[1] for x in coords]
                    lat, long = self.wsg_to_latlon(wsg_x0, wsg_x1, i.properties)

                    lat_centroid = np.mean(lat)
                    long_centroid = np.mean(lat)

                    # Create new set of coordinates
                    new_coords = list(zip(long, lat))

                    # Create New geometry based on latitude longitude
                    new_geom = geom.copy()
                    new_geom['coordinates'] = [[new_coords]]
                    lat_long_geoms.append(new_geom)
                    lat_long_centroids.append((long_centroid, lat_centroid))

                    # Get well know texts 
                    sh_geom = shape(geom)
                    sh_new_geom = shape(new_geom)
                    wkts_wsg85.append(sh_geom.wkt)
                    wkts_lat_long.append(sh_new_geom.wkt)
            except: 
                
                lat_long_geoms.append(np.nan)
                lat_long_centroids.append(np.nan)
                wkts_lat_long.append(np.nan)
                wkts_wsg85.append(np.nan)
                
                pd.DataFrame(i).to_csv('../log/error_{}.csv'.format(datetime.now()))


        return lat_long_geoms, lat_long_centroids, wkts_lat_long, wkts_wsg85