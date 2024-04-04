"""
    LOAD raw data from xportmine
    and PROCESS data
    and RETURN & SAVE data frame with columns:

    Data columns (total 61 columns):
    #   Column                      Dtype
    ---  ------                      -----
    0   ada_month                   datetime64[ns]
    1   arrival_date_actual         datetime64[ns]
    2   arrival_date_delay          int64
    3   arrival_date_estimate       datetime64[ns]
    4   bill_of_lading              object
    5   bill_of_lading_master       object
    6   cargo_count                 float64
    7   carrier_code                object
    8   carrier_name                object
    9   cif                         float64
    10  cif_outliers_off            float64
    11  consignee_address           object
    12  consignee_name              object
    13  container_desc_code         object
    14  container_id                object
    15  container_load_status       object
    16  container_size              object
    17  container_type              object
    18  container_type_of_service   object
    19  country_exp                 object
    20  country_exp_code            object
    21  country_imp                 object
    22  hscode                      object
    23  hscode_02                   object
    24  hscode_02_desc_short        object
    25  hscode_04                   object
    26  hscode_04_desc_short        object
    27  manifest_no                 object
    28  marks_n_numbers             object
    29  notify_party_address        object
    30  notify_party_name           object
    31  place_of_receipt            object
    32  port_of_lading              object
    33  port_of_lading_code         object
    34  port_of_lading_continent    object
    35  port_of_lading_country      object
    36  port_of_lading_lat          float64
    37  port_of_lading_lon          float64
    38  port_of_unlading            object
    39  port_of_unlading_code       object
    40  port_of_unlading_continent  object
    41  port_of_unlading_country    object
    42  port_of_unlading_lat        float64
    43  port_of_unlading_lon        float64
    44  product_desc                object
    45  quantity                    float64
    46  quantity_outliers_off       float64
    47  quantity_unit               object
    48  report_month                period[M]
    49  shipper_address             object
    50  shipper_name                object
    51  teu                         float64
    52  teu_outliers_off            float64
    53  vessel_imo                  object
    54  vessel_name                 object
    55  vessel_type                 object
    56  weight                      float64
    57  weight_kg                   float64
    58  weight_kg_outliers_off      float64
    59  weight_outliers_off         float64
    60  weight_unit                 object
    dtypes: datetime64[ns](3), float64(15), int64(1), object(41), period[M](1)

    @author: mikhail.galkin
"""

# %% Setup ----------------------------------------------------------------------
import sys
import time
import winsound
import warnings

import pandas as pd

from dataprep.clean import clean_headers
from IPython.display import display


# %% Load project's stuff -------------------------------------------------------
sys.path.extend([".", "./.", "././.", "..", "../..", "../../.."])

from mgbol.config import s3_data_local_path

from mgbol.utils import timing
from mgbol.data.xpm.utils import read_xport_us_rar_data

from mgbol.data.xpm.utils import handle_actual_arrival_date
from mgbol.data.xpm.utils import handle_estimated_arrival_date
from mgbol.data.xpm.utils import handle_vessels
from mgbol.data.xpm.utils import handle_ports
from mgbol.data.xpm.utils import handle_listed_data
from mgbol.data.xpm.utils import handle_company
from mgbol.data.xpm.utils import handle_hscode
from mgbol.data.xpm.utils import handle_description
from mgbol.data.xpm.utils import handle_numeric_outliers
from mgbol.data.xpm.utils import handle_weight_outliers
from mgbol.data.xpm.utils import split_column_by_pattern


# %% MAIN -----------------------------------------------------------------------
def main(
    rars_folder_path,
    rars_names=None,
    **kwargs,
):

    DIR_VESSEL_DATA = "z:/S3/ls-aishub-inflated/shipdb/"
    DIR_PORT_DATA = "z:/S3/ls-aishub-inflated/port_data/port_table_dump/"

    FILE_VESSEL_DATA = "shipdb_export_04_2021.zip"
    FILE_PORT_DATA = "port_codes_geo-2022-06-01.csv"
    FILE_HSCODES_TABLE = "ft_hscodes_table.csv"

    path_to_vessel_data = DIR_VESSEL_DATA + FILE_VESSEL_DATA
    path_to_port_data = DIR_PORT_DATA + FILE_PORT_DATA
    path_to_hscodes_table = s3_data_local_path / "hscodes" / FILE_HSCODES_TABLE

    COLS = None  # None if want to read ALL columns
    COLS_DATETIME = ["Estimate Arrival Date", "Actual Arrival Date"]

    warnings.filterwarnings("ignore")
    tic = time.time()

    df = read_xport_us_rar_data(
        rars_folder_path=rars_folder_path,
        rars_names=rars_names,
        cols_to_read=COLS,
        include_country_import=True,
        include_report_month=True,
        drop_dupes=True,
        parse_dates=COLS_DATETIME,
        dtype={
            "Manifest No": str,
            "HS Code": str,  # Very important to be able to save into .parquet
        },
        # nrows=10_000,  #! COMMENTED
    )

    df = clean_headers(df, case="snake", replace={"&": "n"})
    if "unnamed_0" in df.columns:
        df.drop(columns=["unnamed_0"], inplace=True)

    df.rename(
        columns={
            "master_bill_of_lading": "bill_of_lading_master",
            "estimate_arrival_date": "arrival_date_estimate",
            "actual_arrival_date": "arrival_date_actual",
            "weight_in_kg": "weight_kg",
            "hs_code": "hscode",
        },
        inplace=True,
    )

    df = handle_actual_arrival_date(
        df,
        col_ade="arrival_date_estimate",
        col_ada="arrival_date_actual",
        col_report_month="report_month",
    )

    df = handle_estimated_arrival_date(
        df,
        col_ade="arrival_date_estimate",
        col_ada="arrival_date_actual",
        col_delay_name="arrival_date_delay",
        return_delay=True,
    )

    df["ada_month"] = df["arrival_date_actual"] + pd.tseries.offsets.MonthEnd(1)

    df = handle_vessels(
        df,
        path_to_vessel_data,
        col_code="vessel_code",
        col_name="vessel_name",
        col_mode="mode_of_transportation",
        return_match_score=False,
        return_original_cols=False,  #! False
    )

    df = split_column_by_pattern(
        df,
        col_to_split="carrier_sasc_code",
        first_col_name="carrier_code",
        second_col_name="carrier_name",
        pattert_for_split=",",
        fill_na=False,
        return_original_col=False,  #! False
    )

    df = split_column_by_pattern(
        df,
        col_to_split="loading_port",
        first_col_name="port_of_lading_code",
        second_col_name="port_of_lading",
        pattert_for_split=",",
        fill_na=False,
        return_original_col=False,  #! False
    )

    df = split_column_by_pattern(
        df,
        col_to_split="unloading_port",
        first_col_name="port_of_unlading_code",
        second_col_name="port_of_unlading",
        pattert_for_split=",",
        fill_na=False,
        return_original_col=False,  #! False
    )

    df = handle_ports(
        df,
        path_to_port_data,
        port_data_col_code="port_code",
        port_data_cols_join=["lat", "lon", "country", "continent"],
        port_to_handle="port_of_lading",
    )

    df = handle_ports(
        df,
        path_to_port_data,
        port_data_col_code="port_code",
        port_data_cols_join=["lat", "lon", "country", "continent"],
        port_to_handle="port_of_unlading",
    )

    df = split_column_by_pattern(
        df,
        col_to_split="country",
        first_col_name="country_exp_code",
        second_col_name="country_exp",
        pattert_for_split=",",
        fill_na=False,
        return_original_col=False,  #! False
    )

    df = handle_listed_data(
        df,
        cols_to_handle=[
            "container_desc_code",
            "container_id",
            "container_load_status",
            "container_size",
            "container_type",
            "container_type_of_service",
        ],
    )

    df = handle_company(
        df,
        col_name="shipper_name",
        col_address="shipper_address",
        return_original_cols=False,  #! False
        n_blocks="auto",
    )

    df = handle_company(
        df,
        col_name="consignee_name",
        col_address="consignee_address",
        return_original_cols=False,  #! False
        n_blocks="auto",
    )

    df = handle_company(
        df,
        col_name="notify_party_name",
        col_address="notify_party_address",
        return_original_cols=False,  #! False
        n_blocks="auto",
    )

    df = handle_hscode(
        df,
        path_to_hscodes_table,
        col_to_handle="hscode",
        return_cargo_count=True,  #! True
    )

    df = handle_description(
        df,
        col_to_handle="product_desc",
    )

    df = handle_description(
        df,
        col_to_handle="marks_n_numbers",
    )

    df = handle_numeric_outliers(
        df,
        cols_to_handle=["teu", "quantity", "cif"],
        cols_name_suffix="outliers_off",
        outliers_treshold=0.99,
        return_original_cols=True,  #! True
    )

    df = handle_weight_outliers(
        df,
        cols_to_handle=["weight_kg", "weight"],
        cols_name_suffix="outliers_off",
        outliers_treshold=0.99,
        return_original_cols=True,  #! True
    )

    # Convert datetime to string
    df["report_month"] = df["report_month"].dt.strftime("%Y%m")

    winsound.Beep(frequency=3000, duration=400)
    print(f"\nDONE. Preprocessing {timing(tic)}")

    return df


# %% RUN ========================================================================
if __name__ == "__main__":

    RARS_2018 = [  # rebuild
        "201801.US.6003",
        "201802.US.6003",
        "201803.US.6003",
        "201804.US.6003",
        "201805.US.6003",
        "201806.US.6003",
        "201807.US.6003",
        "201808(A).US.6003",
        "201808(B).US.6003",
        "201809(A).US.6003",
        "201809(B).US.6003",
        "201810(A).US.6003",
        "201810(B).US.6003",
        "201811(A).US.6003",
        "201811(B).US.6003",
        "201812(A).US.6003",
        "201812(B).US.6003",
    ]

    RARS_2019 = [
        "201901.US.6003",
        "201902.US.6003",
        "201903.US.6003",
        "201904.US.6003",
        "201905.US.6003",
        "201906.US.6003",
        "201907.US.6003",
        "201908.US.6003",
        "201909.US.6003",
        "201910.US.6003",
        "201911.US.6003",
        "201912.US.6003",
    ]

    RARS_2020 = [
        "202001.US.6003",
        "202002.US.6003",
        "202003.US.6003",
        "202004.US.6003",
        "202005.US.6003",
        "202006.US.6003",
        "202007.US.6003",
        "202008.US.6003",
        "202009.US.6003",
        "202010.US.6003",
        "202011.US.6003",
        "202012.US.6003",
    ]

    RARS_2021 = [
        "202101.US.6003",
        "202102.US.6003",
        "202103.US.6003",
        "202104.US.6003",
        "202105.US.6003",
        "202106.US.6003",
        "202107.US.6003",
        "202108.US.6003",
        "202109.US.6003",
        "202110.US.6003",
        "202111.US.6003",
        "202112.US.6003",
    ]

    RARS_2022 = [
        "202201.US.6003A",
        "202201.US.6003B",
        "202202.US.6003A",
        "202202.US.6003B",
        "202203.US.6003A",
        "202203.US.6003B",
        "202204.US.6003A",
        "202204.US.6003B",
        "202205.US.6003A",
        "202205.US.6003B",
        "202206.US.6003A",
        "202206.US.6003B",
        "202207.US.6003A",
        "202207.US.6003B",
        "202208.US.6003A",
        "202208.US.6003B",
        "202209.US.6003A",
        "202209.US.6003B",
        "202210.US.6003A",
        "202210.US.6003B",
        "202211.US.6003A",
        "202211.US.6003B",
        "202212.US.6003A",
        "202212.US.6003B",
    ]

    RARS = {
        # "2019": RARS_2019,
        # "2020": RARS_2020,
        # "2021": RARS_2021,
        "2022": RARS_2022,
    }

    for year, rar in RARS.items():
        print(f"\n--------------  {year}  ------------------------------------")
        df = main(
            rars_folder_path=s3_data_local_path / "raw/xpm/us",
            rars_names=rar,
        )

        display(df.info(show_counts=True))

        FILE_NAME = "xpm_processed_US"
        DATA_PERIOD = year

        # * Write DF to Parquet ------------------------------------------------
        TO_PARQUET = True
        if TO_PARQUET:
            file_to_save = f"{FILE_NAME}-{DATA_PERIOD}.parquet"
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

        # # * Write DF to CSV.GZ -------------------------------------------------
        # TO_CSV = True
        # if TO_CSV:
        #     file_to_save = f"{FILE_NAME}-{DATA_PERIOD}.csv.gz"
        #     dir_to_save = s3_data_local_path / "processed/xpm/us"
        #     path_to_save = dir_to_save / file_to_save

        #     print(f"Save data as CSV: {path_to_save}")
        #     tic = time.time()
        #     df.to_csv(
        #         path_to_save,
        #         index=False,
        #         encoding="utf-8-sig",
        #         compression="gzip",
        #     )
        #     print(f"Saving {timing(tic)}")

        winsound.Beep(frequency=3000, duration=400)
        print(f"\nDONE.")

# %% Fix the mistake in "202206.US.6003A.rar"
# import rarfile

# rar = "Z:\\S3\\mg-bol\\data\\raw\\xpm\\us\\202206.US.6003A.rar"

# # open rar archive
# with rarfile.RarFile(rar) as r:
#     # open the csv file in the dataset
#     file_name = r.namelist()[0]
#     with r.open(file_name) as file:
#         # read the dataset
#         df = pd.read_csv(
#             file,
#             low_memory=False,
#             na_values=["?", "??", "???", "????", "################"],
#             parse_dates=["Estimate Arrival Date", "Actual Arrival Date"],
#         )
#         print(f"Got #{len(df): ,} records ...")

# df.loc[
#     df["Estimate Arrival Date"].str.contains("4022"), "Estimate Arrival Date"
# ] = df["Estimate Arrival Date"].str.replace("4022", "2022")
# df["Estimate Arrival Date"] = pd.to_datetime(df["Estimate Arrival Date"])

# df.to_csv(
#     "Z:\\S3\\mg-bol\\data\\raw\\xpm\\us\\202206.US.6003A.csv",
#     index=False,
# )
