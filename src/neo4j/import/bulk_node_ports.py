"""
    Creates Data & Header csv files for Ports' Nodes

    Nodes: PortOfLading \ PortOfUnlading
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
def make_data(node_label, node_col_name):
    """
    Template:

    COL_NAME : name of column w/ Name of entity in processed data
    COL_CODE : name of column w/ attribute of entity
    COL_CODE_LAST : name of column been created w/ latest value of attribute

    NODE_LABEL = "Consignee" | "Shipper" | "Notify_Party"
    COL_NAME = f"{NODE_LABEL.lower()}_name"
    COL_CODE = f"{NODE_LABEL.lower()}_address"
    COL_CODE_LAST = "address_last"
    COL_ID_IN_HEADER_NAME = f"name:ID({NODE_LABEL}_ID)"

    NODE_LABEL = "Carrier"
    COL_NAME = f"{NODE_LABEL.lower()}_code"
    COL_CODE = f"{NODE_LABEL.lower()}_name"
    COL_CODE_LAST = "name"
    COL_ID_IN_HEADER_NAME = f"code:ID({NODE_LABEL}_ID)"
    """
    print(f"\nCreate Data & Header files for ({node_label}) node .............")
    warnings.filterwarnings("ignore")

    # node_label = "PortOfLading"
    # node_col_name = "port_of_lading"

    NODE_LABEL = node_label
    NODE_COL_NAME = node_col_name

    COL_ID_IN_HEADER_NAME = f"code:ID({NODE_LABEL}_ID)"

    BULK_IMPORT_NEO4J_FOLDER = s3_neo4j_local_path / "import/xpm"

    PROCESSED_FOLDER_PATH = s3_data_local_path / "processed/xpm/us"
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

    node_col_code = f"{NODE_COL_NAME.lower()}_code"
    node_cols_local = [
        f"{NODE_COL_NAME.lower()}_continent",
        f"{NODE_COL_NAME.lower()}_country",
    ]
    node_cols_geo = [
        f"{NODE_COL_NAME.lower()}_lat",
        f"{NODE_COL_NAME.lower()}_lon",
    ]
    processed_cols_to_read = (
        [NODE_COL_NAME, node_col_code]
        + node_cols_local
        + node_cols_geo
        + [
            "arrival_date_actual",
            "arrival_date_delay",
            "container_id",
            "teu",
            "vessel_type",
        ]
    )

    df = read_xport_processed_data(
        processed_folder_path=PROCESSED_FOLDER_PATH,
        processed_files_names=PROCESSED_FILES_NAMES,
        cols_to_read=processed_cols_to_read,
        drop_dupes=False,  #! Should be False
    )
    # ! Filter the data
    df = df[df["vessel_type"] == "container_ship"]
    df.drop(columns=["vessel_type"], inplace=True)

    print(f"Do some data processing ..........................................")
    # Handle NA's
    cols_na = [node_col_name] + node_cols_local
    df[cols_na] = df[cols_na].fillna(value="N/A")
    df["container_id"] = df["container_id"].fillna(value="XXXXXXXXXXX")

    # Put containers_id into list
    df["container_id"] = df["container_id"].str.split(", ")
    # df["container_count"] = df["container_id"].apply(len)

    print(f"Create spatial variable ...")
    df[node_cols_geo[0]].fillna(0, inplace=True)
    df[node_cols_geo[1]].fillna(0, inplace=True)
    df["location"] = df.apply(
        lambda x: f"{{latitude: {x[node_cols_geo[0]]}, longitude: {x[node_cols_geo[1]]}}}",
        axis=1,
    )
    df.drop(columns=node_cols_geo, inplace=True)

    # Sort before getting last info
    df.sort_values(by=[node_col_code, "arrival_date_actual"], inplace=True)
    # Get last name
    df[node_col_name] = df.groupby([node_col_code])[node_col_name].transform("last")

    print(f"Calculate aggregated stuff .......................................")
    print(f"Calculate containers count ...")
    cols_con = [node_col_code, "arrival_date_actual", "container_id"]
    df_con = df.explode("container_id")[cols_con]
    df_con.groupby(cols_con).size().sort_values()
    df_con.drop_duplicates(inplace=True)
    df_con = df_con.groupby([node_col_code]).size().reset_index(name="container_count")

    print(f"Calculate aggregations ...")
    cols = [node_col_name, node_col_code] + node_cols_local + ["location"]
    df = (
        df.groupby(by=cols, dropna=False)
        .agg(
            delay_count=("arrival_date_delay", lambda x: x[x > 0].count()),
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
    df = pd.merge(df, df_con, how="left")
    del df_con

    # Add the delays' count ratio
    column = "delay_count_ratio"
    loc = df.columns.to_list().index("delay_count") + 1
    value = (df["delay_count"] / df["shipment_count"]).round(4)
    df.insert(loc, column, value)

    # Round the means
    df["delay_days_q50"] = df["delay_days_q50"].round(0).astype(int)
    df["delay_days_q95"] = df["delay_days_q95"].round(0).astype(int)
    df["teu_sum"] = df["teu_sum"].round(1)

    winsound.Beep(frequency=2000, duration=200)

    # Add Node Label
    df[":LABEL"] = NODE_LABEL.replace("_", "")

    print(f"Create .CSV file w/ ({NODE_LABEL}) node header ...")
    header = pd.DataFrame(columns=df.columns)
    header.rename(
        columns={
            node_col_name: "name",
            node_col_code: COL_ID_IN_HEADER_NAME,
            node_cols_local[0]: "loc_continent",
            node_cols_local[1]: "loc_country",
            "location": "location:Point(WGS-84)",
            "delay_count": "delay_count:long",
            "delay_count_ratio": "delay_count_ratio:double",
            "delay_days_q50": "delay_days_q50:long",
            "delay_days_q95": "delay_days_q95:long",
            "shipment_count": "shipment_count:long",
            "shipment_date_first": "shipment_date_first:date",
            "shipment_date_last": "shipment_date_last:date",
            "teu_sum": "teu_sum:double",
            "container_count": "container_count:long",
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
        "PortOfLading": "port_of_lading",
        "PortOfUnlading": "port_of_unlading",
    }
    data = {}
    for node_label, node_col_name in nodes.items():
        result = make_data(node_label, node_col_name)
        data[node_label] = result
    return data


# %% RUN ========================================================================
if __name__ == "__main__":
    _ = main()
