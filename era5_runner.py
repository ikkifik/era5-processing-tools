from era5_reanalysis_v3 import Reanalysis
from raster_boundaries import RasterBoundaries
from era5_grib_extractor import ERA5GribExtractor

from .notification.TelegramMonitoring import duration_formatter, send_telegram_message

if __name__ == "__main__":

    import argparse, time
    parser = argparse.ArgumentParser(description="ERA5 Reanalysis data retrieval")
    parser.add_argument('-rp', '--raster-path', dest="raster_path", type=str, required=True) # raster image
    parser.add_argument('-sy', '--start-year', dest="start_year", type=int, default=2016)
    parser.add_argument('-ey', '--end-year', dest="end_year", type=int, default=2025)
    # parser.add_argument('-y', '--year', dest="year", type=int, default=2015)
    args = parser.parse_args()

    rbound = RasterBoundaries()
    _, rb_filepath = rbound.raster_grid_split(
        source=args.raster_path)
    print(rb_filepath)

    # To get 4-axis boundaries from raster image, please refer to raster_boundaries.py
    # it produces a geojson files that can be inputted to below process

    year = [year for year in range(args.start_year, args.end_year+1)]
    for year in years:
        datenow = datetime.now()
        send_telegram_message(f"ERA5 Reanalysis Start downloading: {str(datetime.strftime(datenow, "%Y-%m-%d %H:%M:%S"))}")

        reanalysis = Reanalysis()
        gribfile = reanalysis.process(
            shape_files=rb_filepath, 
            # year=[y for y in range(int(args.start_year), int(args.end_year)+1)], 
            year=year
        )
        datelater = datetime.now()
        diff = datelater - datenow
        send_telegram_message(f"""
        ERA5 Reanalysis 
        ========================================
        Data downloaded successfully:
        {str(gribfile)}

        Timestamp:
        - start: {str(datetime.strftime(datenow, "%Y-%m-%d %H:%M:%S"))}
        - end: {str(datetime.strftime(datelater, "%Y-%m-%d %H:%M:%S"))}
        - total duration: {str(duration_formatter(int(diff.total_seconds())))}
        """)

        time.sleep(3600*2)



    # extractor = ERA5GribExtractor()
    # extractor.process(filename=gribfile)