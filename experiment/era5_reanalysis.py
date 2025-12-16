
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

    def __init__(self):
        self.uid = os.getenv("ERA5_UID")
        self.api_key = os.getenv("ERA5_API_KEY")
        
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
        
        nowdate = datetime.strftime(datetime.now(), "%Y%m%d_%H%M%S")
        temp_filename = os.path.join(self.temp_dir, f"{nowdate}_{kwargs['year']}{kwargs['month']}.nc")
        temp_metafilename = os.path.join(self.temp_dir, f"{nowdate}_{kwargs['year']}{kwargs['month']}_meta.json")

        c = cdsapi.Client()
            
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
        
        doc = { 
            "area_bounds": area_bounds, 
            "ncfile": temp_filename, 
            "date": f"{kwargs['year']}-{kwargs['month']}-{kwargs['day']}" 
        }
        with open(temp_metafilename, "w") as f:
            json.dump(doc, f)
        
        return temp_filename

    def process(self, shape_files, year, **kwargs):

        month = "6"
        
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
                ncfile = cached_file['ncfile']
            else:
                print("\n[REQ] Online ECMWF Request")
                ncfile = self._retrieve_data(area_bounds=area_bounds, year=year, month=month, day=day)
        except:
            print("\n[MSG] Getting cached .nc file has failed")
            print("[REQ] Online ECMWF Request instead\n")
            ncfile = self._retrieve_data(area_bounds=area_bounds, year=year, month=month, day=day)
            pass
        # Add caching technique (end)

        dataset = xr.open_dataset(ncfile)
        df = dataset.to_dataframe().reset_index()
        gdf = gpd.GeoDataFrame(df[['time', 't2m', 'tp']], geometry=gpd.points_from_xy(df.longitude,df.latitude))
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

        # gdf['time'] = gdf['time'].astype(str)
        # new_col_name = { "t2m": "t2m_score", "tp": "tp_score"}
        # regdf = gdf.rename(columns=new_col_name)
        # regdf.crs = "epsg:4326"
        # regdf = regdf.drop(columns=['time'])

        # regdf['time'] = regdf['time'].apply(lambda t: datetime.strftime(datetime.strptime(t, "%Y-%m-%d %H:%M:%S"), "%Y-%m-%d"))

        # export_location = ""
        # if kwargs.get("export_location"):
        #     export_location = os.path.join(kwargs["export_location"], f"t2mtp_{datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')}.shp.zip")
        #     regdf.to_file(filename=export_location, driver='ESRI Shapefile')

        # os.remove(ncfile)
        return doc
        # return regdf, export_location

if __name__ == "__main__":
    area_study = "shapefile_boundaries/indonesia_temp_raster_full_boundaries.geojson"

    # To get 4-axis boundaries from raster image, please refer to raster_boundaries.py
    # it produces a geojson files that can be inputted to below process

    reanalysis = Reanalysis()
    res = reanalysis.process(
        shape_files=area_study, 
        year=2020, 
        # export_location=
    )