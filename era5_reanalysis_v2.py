
import cdsapi
import geopandas as gpd
import pandas as pd
import xarray as xr
import os
import shutil
import json
from datetime import datetime, date

from dotenv import load_dotenv
load_dotenv()

class Reanalysis:

    def __init__(self):
        # self.uid = os.getenv("ERA5_UID")
        # self.api_key = os.getenv("ERA5_API_KEY")
        
        self.temp_dir = "era5_cache"
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)

    def _search_cache(self, area_bounds, **kwargs):
        cached_data = {}
        try:
            lfs = [f for f in os.listdir("era5_cache") if ".json" in f]
            for lf in lfs:
                selected = json.load(open(os.path.join("era5_cache", lf)))
                if 'date' not in selected:
                    continue
                if selected['area_bounds'] == area_bounds and selected['date'] == kwargs['date']:
                    cached_data = selected
                # elif selected['area_bounds'] == area_bounds:
                #     cached_data = selected
        except Exception as e:
            print("[E] Failed to get cached data", e)
            pass
        
        return cached_data

    def _retrieve_data(self, area_bounds, **kwargs):
        # This program created to run 1-by-1 retrieving data because
        # later the data will changed into raster images.
        
        nowdate = datetime.strftime(datetime.now(), "%Y%m%d_%H%M%S")
        temp_filename = os.path.join(self.temp_dir, f"{nowdate}_{kwargs['year']}{kwargs['month']}.grib")
        temp_metafilename = os.path.join(self.temp_dir, f"{nowdate}_{kwargs['year']}{kwargs['month']}_meta.json")

        dataset = "reanalysis-era5-single-levels"
        request = {
            "product_type": ["reanalysis"],
            "variable": [
                "2m_temperature",
                "total_precipitation"
            ],
            "year": kwargs['year'],
            "month": kwargs['month'],
            "day": kwargs['day'],
            "time": ["13:00"],
            "data_format": "netcdf",
            "download_format": "unarchived",
            'area': [ area_bounds["north"], area_bounds["west"], 
                     area_bounds["south"], area_bounds["east"] ],
        }

        client = cdsapi.Client()
        client.retrieve(dataset, request, temp_filename)
        
        doc = { 
            "area_bounds": area_bounds, 
            "ncfile": temp_filename, 
            "date": f"{kwargs['year']}-{kwargs['month']}-{kwargs['day']}" 
        }
        with open(temp_metafilename, "w") as f:
            json.dump(doc, f)
        
        return temp_filename

    def _retrieve_var(self, dataset):
        t2m_dataset = xr.open_dataset(dataset, engine="cfgrib")
        t2m_df = t2m_dataset.to_dataframe().reset_index()
        tp_dataset = xr.open_dataset(dataset, engine="cfgrib", 
                                     backend_kwargs={'filter_by_keys': {'shortName': 'tp'}})
        tp_df = tp_dataset.to_dataframe().reset_index()

        merged = pd.merge(t2m_df, tp_df[['tp']], on=t2m_df.index, how='left')

        return merged

    def _iterate_date(self, year):

        months = [m for m in range(1, 12+1)]

        d31a = [d for d in range(1,7+1, 2)]
        # d30a = [d for d in range(2,7+1, 2)]
        d31b = [d for d in range(8,12+1, 2)]
        # d30b = [d for d in range(9,12+1, 2)]

        for month in months:
            if (year % 4 == 0) and month == 2:
                days = [d for d in range(1, 29+1)]
            elif month == 2:
                days = [d for d in range(1, 28+1)]
            elif month in d31a or month in d31b:
                days = [d for d in range(1, 31+1)]
            else:
                days = [d for d in range(1, 30+1)]
        
        return months, days

    def process(self, shape_files, **kwargs):
        
        year = kwargs["year"] if kwargs.get("year") else 2020
        if type(year) == list:
            for y in year:
                month, day = self._iterate_date(y)
        else:
            month, day = self._iterate_date(year)
        # month = kwargs["month"] if kwargs.get("month") else 6
        # day = kwargs["day"] if kwargs.get("day") else 15
        
        try:
            if kwargs.get("metadata"):
                print("Using metadata")
                date_acquired = kwargs['metadata']['DATE_ACQUIRED']
                # da = date.fromisoformat(date_acquired)
                da = datetime.strptime(date_acquired, "%Y-%m-%d")
                year, month, day = [da.year, da.month, da.day]
            else:
                # this block will run as a normal state when metadata couldn't be found
                if int(year) > int(datetime.now().year) and int(year) < int(datetime.now().year)-20:
                    # this block of code means that it cannot retrieve data more than the current year
                    # and cannot retrieve data less than 20 years ago
                    return False
                year, month, day = [str(year), str(month), str(day)]
        except Exception as e:
            print("\n[W] Failed to get date, use default date instead.")
            year, month, day = [str(year), str(month), str(day)]
        
        gdf = gpd.read_file(shape_files)

        # Reproject to projected coordinate system
        gdf = gdf.to_crs("EPSG:4326")

        # Get minX, minY, maxX, maxY
        west, south, east, north = gdf.total_bounds
        area_bounds = {"north": north, "west": west, "south": south, "east": east}

        # Add caching technique
        try:
            date_check=f"{year}-{month}-{day}"
            cached_file = self._search_cache(area_bounds=area_bounds, date=date_check)
            if cached_file:
                print(f"\n[i] Using cached ERA5 data: {date_check}")
                gribfile = cached_file['gribfile']
            else:
                print("\n[REQ] Online ECMWF Request")
                gribfile = self._retrieve_data(area_bounds=area_bounds, year=year, month=month, day=day)
        except:
            print("\n[MSG] Getting cached .nc file has failed")
            print("[REQ] Online ECMWF Request instead\n")
            gribfile = self._retrieve_data(area_bounds=area_bounds, year=year, month=month, day=day)
            pass
        # Add caching technique (end)

        df = self._retrieve_var(dataset=gribfile)

        print(df)
        gdf = gpd.GeoDataFrame(df[['valid_time', 't2m', 'tp']], geometry=gpd.points_from_xy(df.longitude,df.latitude))
        # gdf = gdf.to_crs({'init': 'epsg:4326'})
        gdf = gdf.set_crs('epsg:4326')

        # temperature(K) to celcius
        gdf['t2m'] = gdf['t2m'].apply(lambda c: c-273.15)

        # Turn into raster data
        gdf["x"] = gdf["geometry"].x
        gdf["y"] = gdf["geometry"].y

        t2m = (gdf.set_index(["y", "x"]).t2m.to_xarray()).rio.set_crs(4326)
        tp = (gdf.set_index(["y", "x"]).tp.to_xarray()).rio.set_crs(4326)

        dir_path = os.path.join("temp_results", "raster")
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        
        t2m_path = os.path.join(dir_path, f"t2m_{datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')}.TIF")
        t2m.rio.to_raster(t2m_path)

        tp_path = os.path.join(dir_path, f"tp_{datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')}.TIF")
        tp.rio.to_raster(tp_path)

        doc = {
            "t2m_path": str(t2m_path),
            "tp_path": str(tp_path)
        }

        # [end]Turn into raster data

        return doc


if __name__ == "__main__":
    area_study = ""

    # To get 4-axis boundaries from raster image, please refer to raster_boundaries.py
    # it produces a geojson files that can be inputted to below process

    reanalysis = Reanalysis()
    res = reanalysis.process(
        shape_files=area_study, 
        year=[y for y in range(2021, 2025+1)], 
    )