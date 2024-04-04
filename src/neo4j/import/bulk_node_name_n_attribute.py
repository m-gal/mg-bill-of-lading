"""
    Creates Data & Header csv files for Node
    for entity with 'name' and 'attribute'

    Nodes: Consignee \ Shipper \ NotifyParty
    Nodes Type: Identity

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
def make_data(node_label, node_cols):
    """
    Template:

    COL_NAME : name of column w/ Name of entity in processed data
    COL_ATTR : name of column w/ attribute of entity
    COL_ATTR_LAST : name of column been created w/ latest value of attribute

    NODE_LABEL = "Consignee" | "Shipper" | "Notify_Party"
    COL_NAME = f"{NODE_LABEL.lower()}_name"
    COL_ATTR = f"{NODE_LABEL.lower()}_address"
    COL_ATTR_LAST = "address_last"
    COL_ID_IN_HEADER_NAME = f"name:ID({NODE_LABEL}_ID)"

    NODE_LABEL = "Carrier"
    COL_NAME = f"{NODE_LABEL.lower()}_code"
    COL_ATTR = f"{NODE_LABEL.lower()}_name"
    COL_ATTR_LAST = "name"
    COL_ID_IN_HEADER_NAME = f"code:ID({NODE_LABEL}_ID)"
    """
    warnings.filterwarnings("ignore")

    NODE_LABEL = node_label
    COL_NAME = node_cols[0]
    COL_ATTR = node_cols[1]
    COL_ATTR_LAST = node_cols[2]
    COL_ID_IN_HEADER_NAME = f"{node_cols[3]}:ID({NODE_LABEL}_ID)"

    print(f"\nNode: ({NODE_LABEL}) from <{COL_NAME}> & <{COL_ATTR}> ..........")
    print(f"ID column in header: {COL_ID_IN_HEADER_NAME}")

    BULK_IMPORT_NEO4J_FOLDER = s3_neo4j_local_path / "import/xpm"

    PROCESSED_FOLDER_PATH = s3_data_local_path / "processed/xpm"
    PROCESSED_FILES_NAMES = [
        "xpm_processed_US-2019",
        "xpm_processed_US-2020",
        "xpm_processed_US-2021",
        "xpm_processed_US-2022",
    ]

    SAVE_HEADER = True
    SAVE_DATA = True

    tic = time.time()

    bulk_import_header_file_name = f"node_{NODE_LABEL.lower()}-header.csv"
    bulk_import_data_file_name = f"node_{NODE_LABEL.lower()}-data.csv"

    processed_cols_to_read = [
        COL_NAME,
        COL_ATTR,
        "arrival_date_actual",
        "arrival_date_delay",
        "cargo_count",
        "teu",
        "vessel_type",
    ]

    df = read_xport_processed_data(
        processed_folder_path=PROCESSED_FOLDER_PATH,
        processed_files_names=PROCESSED_FILES_NAMES,
        cols_to_read=None,
        drop_dupes=False,  #! Should be False
    )

    print(f"Do some data processing ..........................................")
    # ! Filter the data
    df = df[df["vessel_type"] == "container_ship"]
    # Handle NA's
    df[[COL_NAME, COL_ATTR]] = df[[COL_NAME, COL_ATTR]].fillna(value="N/A")
    df["cargo_count"] = df["cargo_count"].fillna(value=1).astype(int)

    # Sort before getting last info
    df.sort_values(by=[COL_NAME, "arrival_date_actual"], inplace=True)

    # Get last address\attribute
    df[COL_ATTR_LAST] = df.groupby([COL_NAME])[COL_ATTR].transform("last")

    print(f"Calculate aggregated stuff .......................................")
    df = (
        df.groupby(by=[COL_NAME, COL_ATTR_LAST], dropna=False)
        .agg(
            cargo_count_last=("cargo_count", "last"),
            cargo_count_max=("cargo_count", "max"),
            cargo_count_mean=("cargo_count", "mean"),
            # cargo_count_median=("cargo_count", "median"),
            cargo_count_min=("cargo_count", "min"),
            cargo_count_sum=("cargo_count", "sum"),
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
            teu_last=("teu", "last"),
            teu_max=("teu", "max"),
            teu_mean=("teu", "mean"),
            # teu_median=("teu", "median"),
            teu_min=("teu", "min"),
            teu_sum=("teu", "sum"),
        )
        .reset_index()
    )

    # Add the delays' count ratio
    column = "delay_count_ratio"
    loc = df.columns.to_list().index("delay_count") + 1
    value = (df["delay_count"] / df["shipment_count"]).round(4)
    df.insert(loc, column, value)

    # Round the means
    df["delay_days_mean"] = df["delay_days_mean"].round(0).astype(int)
    df["delay_days_q50"] = df["delay_days_q50"].round(0).astype(int)
    df["delay_days_q95"] = df["delay_days_q95"].round(0).astype(int)
    df["cargo_count_mean"] = df["cargo_count_mean"].round(1)
    df["teu_mean"] = df["teu_mean"].round(1)

    winsound.Beep(frequency=2000, duration=200)

    # Add Node Label
    df[":LABEL"] = NODE_LABEL.replace("_", "")

    print(f"Create .CSV file w/ ({NODE_LABEL}) node header ...")
    header = pd.DataFrame(columns=df.columns)
    header.rename(
        columns={
            COL_NAME: COL_ID_IN_HEADER_NAME,
            "cargo_count_last": "cargo_count_last:long",
            "cargo_count_max": "cargo_count_max:long",
            "cargo_count_mean": "cargo_count_mean:double",
            # "cargo_count_median": "cargo_count_median:double",
            "cargo_count_min": "cargo_count_min:long",
            "cargo_count_sum": "cargo_count_sum:long",
            "delay_count": "delay_count:long",
            "delay_count_ratio": "delay_count_ratio:double",
            "delay_days_last": "delay_days_last:long",
            "delay_days_max": "delay_days_max:long",
            "delay_days_mean": "delay_days_mean:long",
            "delay_days_min": "delay_days_min:long",
            "delay_days_q50": "delay_days_q50:long",
            "delay_days_q95": "delay_days_q95:long",
            "shipment_count": "shipment_count:long",
            "shipment_date_first": "shipment_date_first:date",
            "shipment_date_last": "shipment_date_last:date",
            "teu_last": "teu_last:double",
            "teu_max": "teu_max:double",
            "teu_mean": "teu_mean:double",
            # "teu_median": "teu_median:double",
            "teu_min": "teu_min:double",
            "teu_sum": "teu_sum:double",
        },
        inplace=True,
    )

    if SAVE_HEADER:
        print(f"Save file w/ ({NODE_LABEL}) node header ...")
        # Save header
        path = BULK_IMPORT_NEO4J_FOLDER / bulk_import_header_file_name
        header.to_csv(
            path,
            header=True,
            index=False,
        )
        print(f"Header for ({NODE_LABEL}) was saved: {path} ...")

    if SAVE_DATA:
        # Save data
        print(f"Save file w/ ({NODE_LABEL}) nodes data ...")
        path = BULK_IMPORT_NEO4J_FOLDER / f"{bulk_import_data_file_name}.gz"
        df.to_csv(
            path,
            header=False,
            index=False,
            compression="gzip",
        )
        print(f"Data for ({NODE_LABEL}) was saved: {path} ...")

    print(f"\nHeader:")
    display(header.T)

    print(f"\nRandom instances:")
    display(df.sample(4).T)

    print(f"Finally # {len(df):,} of unique ({NODE_LABEL}) nodes ...")
    print(f"Prepare ({NODE_LABEL}) node for Neo4j bulk import {timing(tic)}")
    winsound.Beep(frequency=2000, duration=200)
    print("\nDone.")

    return (df, header)


def main():
    nodes = {
        "Consignee": [
            "consignee_name",
            "consignee_address",
            "address_last",
            "name",
        ],
        "Shipper": [
            "shipper_name",
            "shipper_address",
            "address_last",
            "name",
        ],
        "Carrier": [
            "carrier_code",
            "carrier_name",
            "name_full",
            "name",
        ],
        "NotifyParty": [
            "notify_party_name",
            "notify_party_address",
            "address_last",
            "name",
        ],
    }

    data = {}
    for node_label, node_cols in nodes.items():
        result = make_data(node_label, node_cols)
        data[node_label] = result
    return data


# %% RUN ========================================================================
if __name__ == "__main__":
    _ = main()
