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
-rp "/Users/yylab/ikkifik/research/sentinel-processing-tools/temp_results/data/S2A_MSIL1C_20160210T011802_N0201_R031_T54SWJ_20160210T012044_ndvi.TIF" \
2>&1 | tee "log/output_era5_runner_$(date +%Y%m%d_%H%M%S)".log
```