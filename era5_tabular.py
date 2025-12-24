from pandas import json_normalize
import json
import os

def process(dirpath):

    listdir = os.listdir(dirpath)
    
    docs = []
    for md in listdir:
        metadata_json_path = os.path.join(dirpath, md)

        with open(metadata_json_path) as f:
            metadata_json = json.load(f)
            docs.append(metadata_json)

    df = json_normalize(docs)
    df["type"] = df.apply(lambda x: x["path"].split("_")[0], axis=1)
    df = df.sort_values(by=["metadata.DATE_ACQUIRED"], ascending=True)
    df = df.reset_index(drop=True)

    result_path = os.path.join("dataset")
    if not os.path.exists(result_path):
        os.makedirs(result_path)
    file_path = os.path.join(result_path, 
                f"era5_{df.loc[0, "metadata.DATE_ACQUIRED"]}_{df.loc[len(df)-1, "metadata.DATE_ACQUIRED"]}_tabular.csv")
    df.to_csv(file_path, sep=";")

    return True

if __name__ == "__main__":
    dirpath = "./temp_results/metadata"
    process(dirpath=dirpath)
    
