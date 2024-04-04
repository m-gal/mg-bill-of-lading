"""
    LOAD raw data from the summurizied Bill of Lading
    and GET a first glimpse
    and MAKE Exploratory Data Analysis w\o any changing original data
    and SAVE results of EDA to the [./project/reports/eda].

    * to make the dataprep reports needs dataprep>=0.3.0

    @author: mikhail.galkin
"""

# %% Setup ----------------------------------------------------------------------
import os
import sys
import time
import winsound
import warnings

import pandas as pd

from dataprep.eda import create_report
from IPython.display import display
from pathlib import Path


# %% Load project's stuff -------------------------------------------------------
sys.path.extend([".", "./.", "././.", "..", "../..", "../../.."])

from mgbol.config import s3_data_local_path
from mgbol.config import reports_dir

from mgbol.utils import timing
from mgbol.utils import cols_coerce_to_num
from mgbol.utils import cols_coerce_to_datetime
from mgbol.utils import pd_set_options
from mgbol.utils import pd_reset_options
from mgbol.utils import df_get_glimpse
from mgbol.utils import outliers_get_quantiles
from mgbol.utils import outliers_get_zscores
from mgbol.utils import outliers_ridoff


# %% Custom funcs ---------------------------------------------------------------
def df_preclean_cargo_summary(df, cols_w_outliers):
    print(f"Clean the data for ImportRecords-Cargo-Summary ...")
    df = cols_coerce_to_num(df, ["harmonized_number"])

    ## One-side outliers by positive quantile
    df_q, _ = outliers_get_quantiles(
        df,
        cols_to_check=cols_w_outliers,
        treshold=0.995,
    )
    df = outliers_ridoff(df, df_q)
    df = df.dropna(axis=0, how="all")

    return df


def df_preclean_bol_summary(df, data_file, cols_w_outliers):
    print(f"Clean the data for BillofLadingSummary ...")
    df = cols_coerce_to_num(df, ["harmonized_number"])
    df = cols_coerce_to_datetime(
        df,
        [
            "trade_update_date",
            "run_date",
            "estimated_arrival_date",
            "actual_arrival_date",
        ],
    )
    date = f"{data_file[-12:-8]}-01-01"
    # * Slightly clean data
    print(f"Clean the data ...")
    ## Clean dates
    df = df.loc[df["run_date"] >= date]
    df = df.loc[df["estimated_arrival_date"] >= date]
    df = df.loc[df["actual_arrival_date"] >= date]

    ## One-side outliers by positive quantile
    df_q, _ = outliers_get_quantiles(
        df,
        cols_to_check=cols_w_outliers,
        treshold=0.995,
    )
    df = outliers_ridoff(df, df_q)
    df = df.dropna(axis=0, how="all")

    return df


# %% Main -----------------------------------------------------------------------
def main(
    clean_data=True,
):
    DATA_FILES = [
        "BillofLadingSummary-2016.parquet",
        "BillofLadingSummary-2017.parquet",
        "BillofLadingSummary-2018.parquet",
    ]
    COLS_W_OUTLIERS = [
        "piece_count",
        "harmonized_number",
        "harmonized_value",
        "harmonized_weight",
    ]
    N = 1_000_0  # number of random sample to prepare EDA report

    warnings.filterwarnings("ignore")
    pd_set_options()

    for data_file, year in zip(DATA_FILES, ["2016", "2017", "2018"]):
        print(f"\nLoad {data_file} ... ---------------------------------------")

        file_name = data_file[:-8]
        report_name = f"raw_1M_{file_name}"
        report_title = f"DATA: RAW - {N} samples - {file_name}"
        data_dir = s3_data_local_path / f"raw/sums/{year}"

        tic = time.time()
        df = pd.read_parquet(data_dir / data_file)
        print(f"{data_file} loaded: {timing(tic)}")

        display(df.info(verbose=True, show_counts=True, memory_usage=True))
        # display(df.describe())

        df = df.sample(N).copy()

        # # * Auxiliary plots
        # fgs = (24, 8)
        # df["trade_update_date"].value_counts().sort_index().plot.line(figsize=fgs)
        # df["run_date"].value_counts().sort_index().plot.line(figsize=fgs)
        # df["estimated_arrival_date"].value_counts().sort_index().plot.line(
        #     figsize=fgs
        # )
        # df["actual_arrival_date"].value_counts().sort_index().plot.line(figsize=fgs)

        if clean_data:
            report_title = f"DATA: SLIGHTLY CLEANED - {N} samples - {file_name}"
            if data_file in DATA_FILES[:2]:
                df = df_preclean_cargo_summary(df, COLS_W_OUTLIERS)

            if data_file in DATA_FILES[3:]:
                df = df_preclean_bol_summary(df, data_file, COLS_W_OUTLIERS)

        display(df.info(verbose=True, show_counts=True, memory_usage=True))
        display(df.describe())
        display(df.sample(10))
        # df_get_glimpse(df)

        print(f"EDA: Generating a dataprep report ... ------------------------")
        report = create_report(
            df,
            title=report_title,
            display=[
                "Overview",
                "Variables",
                "Interactions",
                "Correlations",
                "Missing Values",
            ],
        )
        report.save(filename=report_name, to=reports_dir / "eda")
        report.show_browser()

        winsound.Beep(frequency=2000, duration=200)

    pd_reset_options
    print("\nDone.")


# %% RUN ========================================================================
if __name__ == "__main__":
    main(clean_data=True)
