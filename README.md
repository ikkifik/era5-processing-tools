# ERA5 - Reanalysis


### How to use

1. Register yourself to Climate Data Store (ECMWF-Copernicus), then [follow this page](https://cds.climate.copernicus.eu/how-to-api) to install cdsapi.
2. Instead we register our API key to the $HOME directory, we put it within this project directory by creating `.env` file.
```
touch .env
```

3. Then fill the `.env` file with
```
CDSAPI_URL = "https://cds.climate.copernicus.eu/api"
CDSAPI_KEY = "YOUR_CDSAPI_KEY"
```

4. Install the requirements.txt
```
pip install -r requirements.txt
```

5. Run the script (*Explanation coming soon..*)

```
python -u era5_runner.py \
-rp "/Users/yylab/ikkifik/research/sentinel-extraction-tools/results/2025-ofunato/S2A_MSIL1C_20250103T012041_N0511_R031_T54SWJ_20250103T023155.SAFE/GRANULE/L1C_T54SWJ_A049792_20250103T012040/IMG_DATA/T54SWJ_20250103T012041_B01.jp2" \
-sy 2016 \
-ey 2025 \
2>&1 | tee "log/output_era5_runner_$(date +%Y%m%d_%H%M%S)".log
```