import os
import xarray as xr
import pandas as pd

from agage_archive.run import run_all
from agage_archive.config import Paths, data_file_path, open_data_file


def read_repeatability(site, species):
    
    paths = Paths("decc")

    fname = data_file_path(f"{site.upper()}_GCMD_stds.dat", "decc",
                           sub_path=paths.md_path.replace("-modified", "") + "/stds")

    # Read the second and third lines of the file
    with open(fname) as f:
        header = f.readline()
        colnames1 = f.readline()
        colnames2 = f.readline()

    colnames = [(col1 + col2).replace("-", "") for col1, col2 in  zip(colnames1.split(), colnames2.split())]
    colnames = [col[:-1].lower() if col[-1] == "C" else col for col in colnames]

    df = pd.read_csv(fname,
                skiprows=3, delim_whitespace=True,
                names=colnames,
                parse_dates={'datetime': ['date', 'time']},
                date_format='%y%m%d %H%M',
                na_values="nan",
                index_col='datetime')[species]


    # Take running standard deviation with 48 hour window. Ignore NaNs
    df = df.rolling(window=24, min_periods=1, center=True).std()

    return df


def replace_repeatability(site, species):

    paths = Paths("decc")

    fname = data_file_path(f"DECC-GCMD_{site.upper()}_{species.lower()}.nc", "decc", sub_path=paths.md_path)

    with xr.open_dataset(fname, engine="netcdf4") as f:
        ds = f.load()

    # Read new repeatability file
    repeatability_df = read_repeatability(site, species)
    original_repeatability = ds['mf_repeatability'].to_series()

    # Replace the values
    ds['mf_repeatability'] = repeatability_df.reindex(original_repeatability.index, method='nearest')

    # Save the new file
    ds.to_netcdf(fname)


def preprocess():

    sites = ["BSD", "HFD", "RGL"]
    species = ["n2o", "sf6"]

    # Copy MD files from md to md-modified directory, and replace repeatability
    paths = Paths("decc")

    md_modified_path = data_file_path("", "decc", sub_path=paths.md_path)
    md_path = data_file_path("", "decc", sub_path=paths.md_path.replace("-modified", ""))

    for site in sites:
        for sp in species:
            # Copy from md to md-modified
            md_file = os.path.join(md_path, f"DECC-GCMD_{site}_{sp}.nc")
            os.system(f"cp {md_file} {md_modified_path}")

            # Replace repeatability
            replace_repeatability(site, sp)


if __name__ == "__main__":

    preprocess()

    run_all("decc", baseline=False, monthly=False)
