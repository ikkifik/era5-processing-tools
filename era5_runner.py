from era5_reanalysis_v2 import Reanalysis
from raster_boundaries import RasterBoundaries
from era5_grib_extractor import ERA5GribExtractor

if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser(description="ERA5 Reanalysis data retrieval (t2m & tp)")
    parser.add_argument('-rp', '--raster-path', dest="raster_path", type=str, required=True) # raster image
    parser.add_argument('-sy', '--start-year', dest="start_year", type=int, default=2016)
    parser.add_argument('-ey', '--end-year', dest="end_year", type=int, default=2025)
    args = parser.parse_args()

    rbound = RasterBoundaries()
    _, rb_filepath = rbound.raster_grid_split(
        source=args.raster_path)
    print(rb_filepath)

    # To get 4-axis boundaries from raster image, please refer to raster_boundaries.py
    # it produces a geojson files that can be inputted to below process

    reanalysis = Reanalysis()
    gribfile = reanalysis.process(
        shape_files=rb_filepath, 
        year=[y for y in range(int(args.start_year), int(args.end_year)+1)], 
    )

    extractor = ERA5GribExtractor()
    extractor.process(filename=gribfile)