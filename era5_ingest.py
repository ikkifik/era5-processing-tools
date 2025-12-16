
import cdsapi
import geopandas as gpd
import xarray as xr
import os
import shutil
import json
from datetime import datetime, date

from dotenv import load_dotenv
load_dotenv()

class Reanalysis:

    def __init__(self, temp_dir="era5_cache"):
        self.uid = os.getenv("ERA5_UID")
        self.api_key = os.getenv("ERA5_API_KEY")
        
        self.temp_dir = temp_dir
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)

    def _search_cache(self, area_bounds, **kwargs):
        cached_data = {}
        try:
            lfs = [f for f in os.listdir(self.temp_dir) if ".json" in f]
            for lf in lfs:
                selected = json.load(open(os.path.join(self.temp_dir, lf)))
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
        
        nowdate = datetime.strftime(datetime.now(), "%Y%m%d_%H%M%S")
        temp_filename = os.path.join(self.temp_dir, f"{nowdate}_{kwargs['year']}{kwargs['month']}.nc")
        temp_metafilename = os.path.join(self.temp_dir, f"{nowdate}_{kwargs['year']}{kwargs['month']}_meta.json")

        ## Call API
        c = cdsapi.Client(
            "https://cds.climate.copernicus.eu/api/v2", 
            f"{self.uid}:{self.api_key}")
            
        retrieve = c.retrieve(
            'reanalysis-era5-single-levels',
            {
                'product_type': 'reanalysis',
                'format': 'netcdf',
                'variable': ['2m_temperature', 'total_precipitation'],
                'year': kwargs['year'],
                'month': kwargs['month'],
                'day': kwargs['day'],
                'time': '13:00',
                'area': [area_bounds["north"], area_bounds["west"], area_bounds["south"], area_bounds["east"]],
                # -2, 113.5, -3.5, 114.5,
            }, temp_filename)
        
        doc = { "area_bounds": area_bounds, "ncfile": os.path.basename(temp_filename), "date": f"{kwargs['year']}-{kwargs['month']}-{kwargs['day']}" }
        with open(temp_metafilename, "w") as f:
            json.dump(doc, f)
        
        return temp_filename

    def process(self, shape_files, year, **kwargs):
        
        try:
            if kwargs.get("metadata"):
                print("Using metadata")
                date_acquired = kwargs['metadata']['DATE_ACQUIRED']
                # da = date.fromisoformat(date_acquired)
                da = datetime.strptime(date_acquired, "%Y-%m-%d")
                year, month, day = [da.year, da.month, da.day]
            else:
                if int(year) > int(datetime.now().year) and int(year) < int(datetime.now().year)-20:
                    return False
                year, month, day = [str(year), str(month), "15"]
        except Exception as e:
            print("\n[W] Failed to get date, use default date instead.")
            year, month, day = [str(year), str(month), "15"]
        
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
                ncfile = os.path.join(self.temp_dir, cached_file['ncfile'])
            else:
                print("\n[REQ] Online ECMWF Request")
                ncfile = self._retrieve_data(area_bounds=area_bounds, year=year, month=month, day=day)
        except:
            print("\n[MSG] Getting cached .nc file has failed")
            print("[REQ] Online ECMWF Request instead\n")
            ncfile = self._retrieve_data(area_bounds=area_bounds, year=year, month=month, day=day)
            pass
        # Add caching technique (end)
#############################################################################################
        # dataset = xr.open_dataset(ncfile)
        # df = dataset.to_dataframe().reset_index()
        # gdf = gpd.GeoDataFrame(df[['time', 't2m', 'tp']], geometry=gpd.points_from_xy(df.longitude,df.latitude))
        # # gdf = gdf.to_crs({'init': 'epsg:4326'})
        # gdf = gdf.set_crs('epsg:4326')

        # # temperature(K) to celcius
        # gdf['t2m'] = gdf['t2m'].apply(lambda c: c-273.15)

        # # Turn into raster data
        # gdf["x"] = gdf["geometry"].x
        # gdf["y"] = gdf["geometry"].y

        # t2m = (gdf.set_index(["y", "x"]).t2m.to_xarray()).rio.set_crs(4326)
        # tp = (gdf.set_index(["y", "x"]).tp.to_xarray()).rio.set_crs(4326)

        # dir_path = os.path.join("temp_results", "raster")
        # if not os.path.exists(dir_path):
        #     os.makedirs(dir_path)
        
        # t2m_path = os.path.join(dir_path, f"t2m_{datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')}.TIF")
        # t2m.rio.to_raster(t2m_path)

        # tp_path = os.path.join(dir_path, f"tp_{datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')}.TIF")
        # tp.rio.to_raster(tp_path)

        # doc = {
        #     "t2m_path": str(t2m_path),
        #     "tp_path": str(tp_path)
        # }
        # # [end]Turn into raster data
        # return doc
#############################################################################################

if __name__ == "__main__":
    import time
    import random

    # only for ingestion purpose
    with open("batch_retrieve.json", "r") as f:
        areas = json.load(f)
        for area in areas:
            reanalysis = Reanalysis(temp_dir=f"era5_nc_{area['name']}")
            for year in range(2016, 2022+1):
                for month in range(1, 12+1):
                    month = "{0:02d}".format(month)
                    print(f"Getting data on {area['name']} {year}-{month}-15")
                    res = reanalysis.process(shape_files=area["shapefile_path"], year=2020, metadata={ "DATE_ACQUIRED": f"{year}-{month}-15" })
            time.sleep(random.randint(1,5)*60)
            # break