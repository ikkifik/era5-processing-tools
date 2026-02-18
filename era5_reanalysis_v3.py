
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
        self.temp_dir = "era5_cache"
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)

    def _search_cache(self, area_bounds, **kwargs):
        cached_data = {}
        try:
            lfs = [f for f in os.listdir("era5_cache") if ".json" in f]
            for lf in lfs:
                selected = json.load(open(os.path.join("era5_cache", lf)))
                if 'start_date' not in selected:
                    continue
                
                check_date = (selected['start_date'] == kwargs['start_date']) and (selected['end_date'] == kwargs['end_date'])
                if selected['area_bounds'] == area_bounds and check_date:
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

        years = [str(kwargs['year'])]
        months = [f"{number:02d}" for number in range(1, 12+1)]
        days = [f"{number:02d}" for number in range(1, 31+1)]
        times = [f"{number:02d}:00" for number in range(24)]
        variable = [
                "2m_temperature",
                "total_precipitation",
                # additional (2026.02.18)
                "2m_dewpoint_temperature",
                # "soil_temperature_level_1",
                # "volumetric_soil_water_layer_1",
                # "high_vegetation_cover",
                # "low_vegetation_cover",
                # "total_cloud_cover"
            ]
        temp_filename = os.path.join(self.temp_dir, f"{nowdate}_{years[0]}_{"-".join([var for var in variable])}.grib")
        temp_metafilename = os.path.join(self.temp_dir, f"{nowdate}_{years[0]}_{"-".join([var for var in variable])}_meta.json")

        print("=========================================================")
        print("Downloading {year}-{min_month}-{min_day} to {year}-{max_month}-{max_day} ".format(
            year=years[0], 
            min_month=min(months), max_month=max(months),
            min_day=min(days), max_day=max(days),
        ))
        print("Variables: {var}".format(var=variable))

        dataset = "reanalysis-era5-single-levels"
        request = {
            "product_type": ["reanalysis"],
            "variable": variable,
            "year": years,
            "month": months,
            "day": days,
            "time": times,
            "data_format": "grib",
            "download_format": "unarchived",
            'area': [ area_bounds["north"], area_bounds["west"], 
                     area_bounds["south"], area_bounds["east"] ],
        }

        client = cdsapi.Client()
        client.retrieve(dataset, request, temp_filename)
        
        doc = { 
            "area_bounds": area_bounds, 
            "data_path": temp_filename, 
            "start_date": f"{year}-{min(months)}-{min(days)}__{min(times)}",
            "end_date": f"{year}-{max(months)}-{max(days)}__{max(times)}",
        }
        with open(temp_metafilename, "w") as f:
            json.dump(doc, f)
        
        return temp_filename

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
        
        year = kwargs["year"] if kwargs.get("year") else 2020 #TODO: Need a replacement
        
        gdf = gpd.read_file(shape_files)

        # Reproject to projected coordinate system
        gdf = gdf.to_crs("EPSG:4326")

        # Get minX, minY, maxX, maxY
        west, south, east, north = gdf.total_bounds
        area_bounds = {"north": north, "west": west, "south": south, "east": east}
    
        gribfile = self._retrieve_data(area_bounds=area_bounds, year=year)
        
        return gribfile


if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser(description="ERA5 Reanalysis data retrieval (t2m & tp)")
    parser.add_argument('-rp', '--raster-path', dest="raster_path", type=str, required=True)
    parser.add_argument('-y', '--year', dest="year", type=int, default=2015)
    args = parser.parse_args()

    # To get 4-axis boundaries from raster image, please refer to raster_boundaries.py
    # it produces a geojson files that can be inputted to below process

    reanalysis = Reanalysis()
    res = reanalysis.process(
        shape_files=args.raster_path, 
        year=args.year, 
    )