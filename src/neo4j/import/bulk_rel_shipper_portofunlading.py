"""
    Relationship: (Shipper) - [SHIPS_TO] -> (PortOfUnlading)
    Relationship Type: timestamped structural
    PREPARE 2 CSV files (Header + Data) for bulk data importing into Neo4j
    SAVE all of them as .csv

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
from mgbol.config import s3_neo4j_local_path

from mgbol.utils import timing
from mgbol.neo4j.xpm.utils import read_xport_processed_data


# %% MAIN -----------------------------------------------------------------------
def main():
    warnings.filterwarnings("ignore")

    TYPE_OF_RELS = "SHIPS_TO"

    NODE_OUT = "Shipper"
    NODE_OUT_COL_NAME = "shipper_name"

    NODE_IN = "PortOfUnlading"
    NODE_IN_COL_NAME = "port_of_unlading_code"

    SAVE_HEADER = True
    SAVE_DATA = True

    BULK_IMPORT_NEO4J_FOLDER = s3_neo4j_local_path / "import/xpm"

    PROCESSED_FOLDER_PATH = s3_data_local_path / "processed/xpm/us"
    PROCESSED_FILES_NAMES = [
        "xpm_processed_US-2019",
        "xpm_processed_US-2020",
        "xpm_processed_US-2021",
        "xpm_processed_US-2022",
    ]

    tic = time.time()

    bulk_import_header_file_name = f"rel_{NODE_OUT.lower()}-{NODE_IN.lower()}-header.csv"
    bulk_import_data_file_name = f"rel_{NODE_OUT.lower()}-{NODE_IN.lower()}-data.csv"

    processed_cols_to_read = [
        NODE_OUT_COL_NAME,
        NODE_IN_COL_NAME,
        "arrival_date_actual",
        "arrival_date_delay",
        "container_id",
        "teu",
        "vessel_type",
    ]

    print(f"\nCreate relations: ({NODE_OUT}) - [{TYPE_OF_RELS}] -> ({NODE_IN})")
    df = read_xport_processed_data(
        processed_folder_path=PROCESSED_FOLDER_PATH,
        processed_files_names=PROCESSED_FILES_NAMES,
        cols_to_read=processed_cols_to_read,
        drop_dupes=False,  #! Should be False
    )
    # ! Filter the data
    df = df[df["vessel_type"] == "container_ship"]

    print(f"Do some data processing ..........................................")
    # Handle NA's
    df[[NODE_OUT_COL_NAME, NODE_IN_COL_NAME]] = df[[NODE_OUT_COL_NAME, NODE_IN_COL_NAME]].fillna(
        "N/A"
    )
    df["container_id"] = df["container_id"].fillna(value="XXXXXXXXXXX")

    # Put containers_id into list
    df["container_id"] = df["container_id"].str.split(", ")

    # # Sort before getting last info
    # df.sort_values(
    #     by=[
    #         NODE_OUT_COL_NAME,
    #         NODE_IN_COL_NAME,
    #         "arrival_date_actual",
    #     ],
    #     inplace=True,
    # )

    print(f"Calculate aggregated stuff .......................................")
    print(f"Calculate containers count ...")
    cols_con = [
        NODE_OUT_COL_NAME,
        NODE_IN_COL_NAME,
        "arrival_date_actual",
        "container_id",
    ]
    df_con = df.explode("container_id")[cols_con]
    df_con.groupby(cols_con).size().sort_values()
    df_con.drop_duplicates(inplace=True)
    df_con = (
        df_con.groupby([NODE_OUT_COL_NAME, NODE_IN_COL_NAME])
        .size()
        .reset_index(name="container_count")
    )

    print(f"Calculate aggregations ...")
    df = (
        df.groupby(by=[NODE_OUT_COL_NAME, NODE_IN_COL_NAME], dropna=False)
        .agg(
            delay_count=("arrival_date_delay", lambda x: x[x > 0].count()),
            delay_days_last=("arrival_date_delay", "last"),
            delay_days_max=("arrival_date_delay", "max"),
            delay_days_mean=("arrival_date_delay", "mean"),
            delay_days_min=("arrival_date_delay", "min"),
            delay_days_q50=("arrival_date_delay", "median"),
            delay_days_q95=("arrival_date_delay", lambda x: x.quantile(0.95)),
            shipment_count=("arrival_date_actual", "size"),
            shipment_date_first=("arrival_date_actual", "min"),
            shipment_date_last=("arrival_date_actual", "max"),
            teu_sum=("teu", "sum"),
        )
        .reset_index()
    )

    # Add container count
    df = pd.merge(
        df,
        df_con,
        how="left",
        left_on=[NODE_OUT_COL_NAME, NODE_IN_COL_NAME],
        right_on=[NODE_OUT_COL_NAME, NODE_IN_COL_NAME],
    )
    del df_con

    # Add the delays' count ratio
    column = "delay_count_ratio"
    loc = df.columns.to_list().index("delay_count") + 1
    value = (df["delay_count"] / df["shipment_count"]).round(4)
    df.insert(loc, column, value)

    # Add the inverted delay_days_q95
    column = "delay_days_q95i"
    loc = df.columns.to_list().index("delay_days_q95") + 1
    value = [1 if x <= 0 else round(1 / x, 4) for x in df["delay_days_q95"]]
    df.insert(loc, column, value)

    # Round the means
    df["delay_days_mean"] = df["delay_days_mean"].round(0).astype(int)
    df["delay_days_q50"] = df["delay_days_q50"].round(0).astype(int)
    df["delay_days_q95"] = df["delay_days_q95"].round(0).astype(int)
    df["teu_sum"] = df["teu_sum"].round(1)

    # Add timestamps for relationship with thr magic number very large long int
    # Because Neo4j does not have
    df[".from:date"] = df["shipment_date_first"] + pd.tseries.offsets.MonthBegin(-1)
    # '~' means The End Of Time
    df[".to"] = "~"

    # Add Relationship Type
    # df.insert(2, ":TYPE", TYPE_OF_RELS.upper())
    df[":TYPE"] = TYPE_OF_RELS.upper()

    print(f"Create .CSV file w/ ({TYPE_OF_RELS}) relationship header ...")
    header = pd.DataFrame(columns=df.columns)
    header.rename(
        columns={
            NODE_OUT_COL_NAME: f":START_ID({NODE_OUT}_ID)",
            NODE_IN_COL_NAME: f":END_ID({NODE_IN}_ID)",
            "delay_count": "delay_count:long",
            "delay_count_ratio": "delay_count_ratio:double",
            "delay_days_last": "delay_days_last:long",
            "delay_days_max": "delay_days_max:long",
            "delay_days_mean": "delay_days_mean:long",
            "delay_days_min": "delay_days_min:long",
            "delay_days_q50": "delay_days_q50:long",
            "delay_days_q95": "delay_days_q95:long",
            "delay_days_q95i": "delay_days_q95i:double",
            "shipment_count": "shipment_count:long",
            "shipment_date_first": "shipment_date_first:date",
            "shipment_date_last": "shipment_date_last:date",
            "teu_sum": "teu_sum:double",
            "container_count": "container_count:long",
        },
        inplace=True,
    )
    display(header.T)

    if SAVE_HEADER:
        print(f"Save file w/ ({TYPE_OF_RELS}) relationship header ...")
        # Save header
        path = BULK_IMPORT_NEO4J_FOLDER / bulk_import_header_file_name
        header.to_csv(
            path,
            header=True,
            index=False,
        )
        print(f"Header for ({TYPE_OF_RELS}) was saved: {path} ...")

    if SAVE_DATA:
        # Save data
        print(f"Save file w/ ({TYPE_OF_RELS}) relationships data ...")
        path = BULK_IMPORT_NEO4J_FOLDER / f"{bulk_import_data_file_name}.gz"
        df.to_csv(
            path,
            header=False,
            index=False,
            compression="gzip",
        )
        print(f"Data for ({TYPE_OF_RELS}) was saved: {path} ...")

    display(df.sample(4).T)

    print(f"Finally # {len(df):,} of unique ({TYPE_OF_RELS}) relationships ...")
    print(f"Prepare ({TYPE_OF_RELS}) rels for Neo4j bulk import {timing(tic)}")
    winsound.Beep(frequency=2000, duration=200)
    print("\nDone.")


# %% RUN ========================================================================
if __name__ == "__main__":
    main()
