"""
This script downloads the yearly population and median household income
estimates from the American Community Survey for all census designated
places in Alaska.

It then exports it as a CSV for use in other projects.
"""

# Import standard packages
import json
import os
from pathlib import Path

# Import third-party packages
import httpx
import polars as pl

# Import user packages
from src.credentials import USCensusAPI

# Set up the API Query
host_name = "https://api.census.gov/data/"
dataset = "/acs/acs5"
g = "?get="
var = "NAME,B01003_001E,B19013_001E"  # Name, Estimated Total Population, Median Household Income (in year inflation-adjusted dollars)
location = "&for=place:*&in=state:02"
api_string = f"{USCensusAPI.api_string}"  # Import api key with the '&key=' prefix

## Create an empty dataframe
acs5_ak_places = pl.DataFrame(
    schema={
        "NAME": str,
        "B01003_001E": pl.Int64,
        "B19013_001E": pl.Int64,
        "state": str,
        "place": str,
        "year": pl.Int32,
    }
)

# Query Data from the US Census American Community Survey (5-year estimates)
for year in range(2001, 2022):
    year = str(year)
    query_url = f"{host_name}{year}{dataset}{g}{var}{location}{api_string}"
    response = httpx.get(query_url)
    try:
        n = len(response.json())
        print(f"Found {n} records in {year}")
        json_object = json.dumps(response.json())
        export_path = f".data/census-json/acs5-ak-place-{year}.json"
        with open(export_path, "w") as outfile:
            outfile.write(json_object)
        print(f"Wrote JSON Object to {export_path}")

        tmp = pl.DataFrame(response.json()).transpose()
        tmp = tmp.rename(tmp.head(1).to_dicts().pop()).slice(1)
        tmp = tmp.with_columns(
            pl.lit(int(year)).alias("year"),
            pl.col("B01003_001E").cast(pl.Int64),
            pl.col("B19013_001E").cast(pl.Int64),
        )
        acs5_ak_places = pl.concat([acs5_ak_places, tmp])
        print(f"Successfully merged in the {year} data")

    except Exception:
        print(f"Found no records in {year}")


acs5_ak_places = acs5_ak_places.rename(
    {"B01003_001E": "population", "B19013_001E": "median_hh_income", "NAME": "name"}
)

export_path = os.path.join(os.curdir, ".data", "us-census-acs5yr-ak.csv")
acs5_ak_places.write_csv(export_path)

print(f"Wrote data to {export_path}")
