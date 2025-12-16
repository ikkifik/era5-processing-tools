from shapely import geometry
from rasterio.crs import CRS
import rasterio as rio
import rioxarray as rxr
import geopandas as gpd
import os

class RasterBoundaries:
    def __init__(self):
        self.temp_path = "temp_results"
        if not os.path.exists(self.temp_path):
            os.makedirs(self.temp_path)

    def raster_grid_split(self, source):

        def _get_whole_boundaries(maxX, minX, maxY, minY):
            left_bottom = (minX, minY)
            left_top = (minX, maxY)
            right_top = (maxX, maxY)
            right_bottom = (maxX, minY)

            geom = geometry.Polygon([left_bottom, left_top, right_top, right_bottom])
            geomdf = gpd.GeoDataFrame(index=[0], geometry=[geom], crs="EPSG:4326")

            path_full_boundaries = os.path.join(self.temp_path, "temp_raster_full_boundaries.geojson")
            geomdf.to_file(path_full_boundaries, driver="GeoJSON")

            return geomdf, path_full_boundaries
        
        imagery = rio.open(source)
        raster = rxr.open_rasterio(source, masked=True).squeeze()
        raster_new = raster.rio.reproject(CRS.from_string('EPSG:4326'))

        maxX = raster_new.coords['x'].max(dim=["x"]).to_dict()['data']
        minX = raster_new.coords['x'].min(dim=["x"]).to_dict()['data']
        maxY = raster_new.coords['y'].max(dim=["y"]).to_dict()['data']
        minY = raster_new.coords['y'].min(dim=["y"]).to_dict()['data']

        # Create a fishnet
        x, y = (minX, minY)
        geom_array = []

        # Polygon Size
        square_size = tuple(map(lambda i, j: i - j, (maxX, minY), (minX, minY)))
        square_size = square_size[0]/int(3)

        while y <= maxY:
            while x <= maxX:
                geom = geometry.Polygon([(x,y), (x, y+square_size), (x+square_size, y+square_size), (x+square_size, y), (x, y)])
                geom_array.append(geom)
                x += square_size
            x = minX
            y += square_size

        fishnet = gpd.GeoDataFrame(geom_array, columns=['geometry']).set_crs('EPSG:4326')
        
        # Select grids within area study
        wbounds, wbounds_path = _get_whole_boundaries(maxX, minX, maxY, minY)
        within = gpd.overlay(wbounds, fishnet, how='intersection') #.reset_index()
        within_check = within.copy()
        
        # temporary change projection to find the area total
        within_check = within_check.to_crs({'init': 'epsg:3857'})
        within_check["area"] = within_check['geometry'].area/ 10**6
        within_check = within_check[within_check['area'] != 0]
        within_check = within_check[within_check['area'] > within_check['area'].mean()].reset_index()

        # re-project to wgs84
        within_check = within_check.to_crs({'init': 'epsg:4326'})
        within_check = within_check.drop(columns=['index'])

        return within_check, wbounds_path

if __name__ == "__main__":
    rbound = RasterBoundaries()
    _, total_bounds = rbound.raster_grid_split(
        source="/Users/yylab/ikkifik/research/sentinel-extraction-tools/results/2025-ofunato/S2A_MSIL1C_20250103T012041_N0511_R031_T54SWJ_20250103T023155.SAFE/GRANULE/L1C_T54SWJ_A049792_20250103T012040/IMG_DATA/T54SWJ_20250103T012041_B01.jp2")
    print(total_bounds)