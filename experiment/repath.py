import json
import os

era5_cache = [os.path.join("era5_cache", d) for d in os.listdir("era5_cache") if "meta.json" in d]

for ec in era5_cache:
    with open(ec, "r") as f:
        meta = json.load(f)
        meta['ncfile'] = os.path.basename(meta['ncfile'])
    
    with open(ec, "w") as f:
        json.dump(meta, f)
