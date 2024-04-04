"""
    Creates consolidated data set from already processed data
    with deduplicated entities' names

    @author: mikhail.galkin
"""

# %% Setup ----------------------------------------------------------------------
import sys
import time

import winsound
import warnings

import pandas as pd
from IPython.display import display

# %% Load project's stuff -------------------------------------------------------
sys.path.extend([".", "./.", "././.", "..", "../..", "../../.."])

from mgbol.config import s3_data_local_path

from mgbol.utils import timing
from mgbol.neo4j.xpm.utils import read_xport_processed_data
from mgbol.data.xpm.utils import handle_company


# %% MAIN -----------------------------------------------------------------------
def pool_names(entity_type):
    # entity_type = "notify_party"

    FILE_NAME = f"xpm_pooled_{entity_type}_US"

    PROCESSED_FOLDER_PATH = s3_data_local_path / "processed/xpm/us"
    PROCESSED_FILES_NAMES = [
        "xpm_processed_US-2019",
        "xpm_processed_US-2020",
        "xpm_processed_US-2021",
        "xpm_processed_US-2022",
    ]

    COLS_TO_READ = [
        f"{entity_type}_name",
        f"{entity_type}_address",
    ]

    df = read_xport_processed_data(
        processed_folder_path=PROCESSED_FOLDER_PATH,
        processed_files_names=PROCESSED_FILES_NAMES,
        cols_to_read=COLS_TO_READ,
        drop_dupes=True,
    )

    col_name = COLS_TO_READ[0]
    col_address = COLS_TO_READ[1]
    df = handle_company(
        df,
        col_name=col_name,
        col_address=col_address,
        return_original_cols=False,  #! False
        n_blocks="auto",
    )
    df = df.drop_duplicates()

    # * Write DF to Parquet ----------------------------------------------------
    TO_PARQUET = True
    if TO_PARQUET:
        file_to_save = f"{FILE_NAME}.parquet"
        dir_to_save = s3_data_local_path / "processed/xpm/us"
        path_to_save = dir_to_save / file_to_save

        print(f"Save data as PARQUET: {path_to_save}")
        tic = time.time()
        df.to_parquet(
            path=path_to_save,
            engine="auto",
            compression="snappy",
            index=False,
        )
        print(f"Saving {timing(tic)}")

    return df


# %% RUN ========================================================================
if __name__ == "__main__":
    dfn = pool_names(entity_type="notify_party")
    dfs = pool_names(entity_type="shipper")
    dfc = pool_names(entity_type="consignee")
