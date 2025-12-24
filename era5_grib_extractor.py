import xarray as xr
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import os, json
from datetime import datetime, timedelta
# Extract metadata
import rasterio
import rasterio.features
import rasterio.warp
import numpy as np
from tqdm import tqdm

import warnings
warnings.filterwarnings('ignore')

class ERA5GribExtractor:
    def __init__(self):
        pass

    def __stat_value(self, band_entry):
            band = rasterio.open(band_entry)
            val = band.read(1).astype('float32')
            # Solve nan value
            val[val==np.nan] = 0
            collect = {
                "max": float(np.nanmax(val)),
                "min": float(np.nanmin(val)),
                "mean": float(np.nanmean(val)),
                "med": float(np.nanmedian(val)),
            }
            
            return collect

    def __extract_geom(self, filename):
        with rasterio.open(filename) as data:
            # Read the dataset's valid data mask as a ndarray.
            mask = data.dataset_mask()
            # Extract feature shapes and values from the array.
            # for geom, val in rasterio.features.shapes(mask, transform=data.transform):
            geom = [geom for geom, val in rasterio.features.shapes(mask, transform=data.transform)][0]

            # Transform shapes from the dataset's own coordinate
            # reference system to CRS84 (EPSG:4326).
            geom = rasterio.warp.transform_geom(data.crs, 'EPSG:4326', geom, precision=6)
            wkt_coords = [f"{(coord[0])} {(coord[1])}" for coord in geom["coordinates"][0]]
            wkt = f"POLYGON ({tuple(wkt_coords)})".replace('\'', '')

            return wkt

    def __export_metadata(self, data_path, date_acquired):

        result = {}
        result.update({"path": os.path.basename(data_path)})
        result.update({"metadata": {
                "DATE_ACQUIRED": date_acquired,
                "SPACECRAFT_ID": "ERA5",
                "WKT_COORDS": self.__extract_geom(data_path),
                "FACTORS": os.path.basename(data_path).split("_")[0]
            }})
        result.update({"stat_value": self.__stat_value(data_path)})

        meta_dir_path = os.path.join("temp_results", "metadata")
        if not os.path.exists(meta_dir_path):
            os.makedirs(meta_dir_path)

        metadata_filename = os.path.join(meta_dir_path, "metadata_"+os.path.basename(data_path.replace("TIF", "json")))
        with open(metadata_filename, "w") as f:
            json.dump(result, f, indent=4)
        
        return result

    def process(self, filename):

        # xarray dataset open
        t2m_dataset = xr.open_dataset(filename, engine="cfgrib")
        tp_dataset = xr.open_dataset(filename, engine="cfgrib", backend_kwargs={'filter_by_keys': {'shortName': 'tp'}})

        # xarray dataframe making
        tp_df = tp_dataset.to_dataframe().reset_index()
        t2m_df = t2m_dataset.to_dataframe().reset_index()
        # convert temperature (t2m) kelvin to celcius
        t2m_df['t2m'] = t2m_df['t2m'].apply(lambda c: c-273.15)
        # join both separate dataframe into one dataframe
        join = pd.merge(t2m_df, tp_df[['tp']], on=t2m_df.index, how='left')

        # convert dataframe to geodataframe to obtain the geometry features
        gdf = gpd.GeoDataFrame(
            join, geometry=gpd.points_from_xy(join.longitude, join.latitude), crs="EPSG:4326"
        )

        # Drop unused features, create validated datetime format
        gdf = gdf.drop(columns=["number", "step", "time", "surface", "key_0", "latitude", "longitude"])
        gdf['date'] = pd.to_datetime(gdf['valid_time']).dt.date
        gdf['date'] = pd.to_datetime(gdf['date'], format='%Y-%m-%d')
        gdf['time'] = pd.to_datetime(gdf['valid_time']).dt.time

        # ===========================================================================================
        # Do some grouping/aggregation based on the date
        print("Aggregating data")
        date_list = pd.date_range(gdf["date"].min(), gdf["date"].max()-timedelta(days=1),freq='d')

        df_list = []
        for idx, d in enumerate(tqdm(date_list)):
            # get (split) data from the same date
            df_by_date = gdf.loc[(gdf['date'] == d)]
            # group data by geometry, this will concise/aggregate data based on its geometry
            gdf_agg = df_by_date.groupby(by="geometry").agg(
                t2m_mean=('t2m', 'mean'), 
                tp_mean=('tp', 'mean'),
                date_min=('date', 'min')
            )
            # cleaning the data, flatten the df, bring back the old column name
            gdf_agg = gdf_agg.reset_index()
            gdf_agg["t2m"] = gdf_agg[["t2m_mean"]]
            gdf_agg["tp"] = gdf_agg[["tp_mean"]]
            gdf_agg["date"] = gdf_agg[["date_min"]]

            # drop the old (agg) version of columns
            gdf_agg = gdf_agg.drop(columns=["t2m_mean", "tp_mean", "date_min"])
            clean_gdf = gpd.GeoDataFrame(gdf_agg[['date', 't2m', 'tp']], geometry=gdf_agg.geometry)
            # gdf = gdf.to_crs({'init': 'epsg:4326'})
            clean_gdf = clean_gdf.set_crs('epsg:4326')
            df_list.append(clean_gdf)


        # ===========================================================================================
        # Loop through collection of grouped dataframe
        print("Generating raster image")
        for idx, dfl in enumerate(tqdm(df_list)):

            gdf = dfl
            # Turn into raster data
            gdf["x"] = gdf["geometry"].x
            gdf["y"] = gdf["geometry"].y

            t2m = (gdf.set_index(["y", "x"]).t2m.to_xarray()).rio.set_crs(4326)
            tp = (gdf.set_index(["y", "x"]).tp.to_xarray()).rio.set_crs(4326)

            dir_path = os.path.join("temp_results", "raster")
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            
            t2m_path = os.path.join(dir_path, f"t2m_{datetime.strftime(gdf["date"][0], '%Y%m%d')}.TIF")
            t2m.rio.to_raster(t2m_path)
            self.__export_metadata(data_path=t2m_path, date_acquired=datetime.strftime(gdf["date"][0], '%Y-%m-%d'))
            
            tp_path = os.path.join(dir_path, f"tp_{datetime.strftime(gdf["date"][0], '%Y%m%d')}.TIF")
            tp.rio.to_raster(tp_path)
            self.__export_metadata(data_path=tp_path, date_acquired=datetime.strftime(gdf["date"][0], '%Y-%m-%d'))
        
        print("All process has completed!")
    
if __name__ == "__main__":
    gribfile = "/Users/yylab/ikkifik/research/era5-reanalysis/_archives/ofunato_t2m_tp.grib"
    extractor = ERA5GribExtractor()
    extractor.process(filename=gribfile)