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
