""" Contains the functions used for XPORTMINE data processing

    @author: mikhail.galkin
"""

# %% Import needed python libraryies and project config info
import time
import winsound
import numpy as np
import pandas as pd
import rarfile
import multiprocessing as mp

from fuzzywuzzy import fuzz  # for fuzzy matching scorer
from functools import partial
from string import printable

from pathlib import Path
from IPython.display import display
from pprint import pprint

from mgbol.utils import timing
from mgbol.utils import drop_duplicated
from mgbol.utils import outliers_get_quantiles
from mgbol.utils_special import do_fuzzy_matching
from mgbol.utils_special import do_parallel_works_with_list
from mgbol.utils_special import preprocess_column_to_group
from mgbol.utils_special import do_ngram_grouping


# ------------------------------------------------------------------------------
# -------------------------- U T I L I T I E S ---------------------------------
# ------------------------------------------------------------------------------
def valid_int(x):
    try:
        int(x)
        return True
    except ValueError:
        return False


# ------------------------------------------------------------------------------
# --------------- L O A D I N G   &   S A V I N G    S T U F F -----------------
# ------------------------------------------------------------------------------
def read_xport_us_rar_data(
    rars_folder_path,
    rars_names: list,  # or None
    cols_to_read: list,  # or None
    include_country_import=True,
    include_report_month=True,
    drop_dupes=False,
    **kwargs,
):
    """Read CSV-files in RAR from the Xportmine data provider
    and concatenate the data into one DF

    Args:
        rars_folder_path (Path): Path to folder with RAR archives
        rars_names (list of str or None): RAR files' names w/o extension '.rar'
        cols_to_read (list of str or None): Columns' names to read
        include_country_import (bool): Include column w/ importing country
        include_report_month (bool): Include column w/ month of BoL report
        drop_dupes (bool): Either check for duplicates
        **kwargs: kwargs for pandas.read_csv()
    Returns:
        Pandas DataFrame : Data combined into one DF
    """
    print(f"\nRead the Xportmine RARs data ...................................")
    tic_main = time.time()

    # Get full paths to rar archives
    if rars_names is None:
        # Get all RAR archives in folder
        p = rars_folder_path.glob("**/*.rar")
        rars = [x for x in p if x.is_file()]
    else:
        # Constract list of Paths
        rars = [rars_folder_path / f"{r}.rar" for r in rars_names]

    # Get label for importing country
    country_imp = rars_folder_path.stem.upper()

    df = pd.DataFrame()  # empty DF for interim result

    print(f"\n---- Get BoL data for the columns:")
    pprint("All columns..." if cols_to_read is None else cols_to_read)

    # We want exclude some technical columns
    if cols_to_read is None:
        cols_to_read = lambda x: x not in ["day", "month", "year"]

    # Path through all RARs
    for rar in rars:
        tic = time.time()
        print(f"\n---- Get BoL data from the <{rar}> ...")

        # open rar archive
        with rarfile.RarFile(rar) as r:
            # open the csv file in the dataset
            file_name = r.namelist()[0]
            with r.open(file_name) as file:
                # read the dataset
                _df = pd.read_csv(
                    file,
                    usecols=cols_to_read,
                    low_memory=False,
                    na_values=["?", "??", "???", "????", "################"],
                    **kwargs,
                )
                print(f"Got #{len(_df): ,} records ...")

                # Handle the NaNs
                n = len(_df)
                _df.dropna(how="all", inplace=True)
                print(f"Dropped NaNs # {n - len(_df)} ...")

                if drop_dupes:
                    _df = drop_duplicated(_df)

                if include_country_import:
                    _df["country_imp"] = country_imp

                if include_report_month:
                    _df["report_month"] = pd.to_datetime(
                        Path(rar).stem.split(".")[0][:6],
                        format="%Y%m",
                    ).to_period("M")

                # #! Check dtypes. COMMENTED
                # print(f"INFO: LOADED data:")
                # display(_df.info())

                # Concatenate the data
                df = pd.concat([df, _df], axis=0, ignore_index=True)
                del _df

                # #! Check dtypes. COMMENTED
                # print(f"INFO: CONCATENATED data:")
                # display(df.info())

        # Timing for rar
        print(f"The <{rar}> was read {timing(tic)}")

    # In the <202010.US.6003.csv> here is the 'System Identity Id' column
    if "System Identity Id" in df.columns:
        print(f"\nDrop the 'System Identity Id' column ...")
        df.drop(columns=["System Identity Id"], inplace=True)

    if drop_dupes:
        # Get columns subset to check for duplicated
        subset = [x for x in df.columns.to_list() if not x.startswith("_")]
        print(f"\nCheck the duplicates for the final DF for the columns:...")
        display(subset)
        df = drop_duplicated(df, subset)

    print(f"\nFinal dataset has # {len(df):,} records ...")
    # display(df.info(show_counts=True))
    winsound.Beep(frequency=2000, duration=200)
    print(f"Totally read all RARs {timing(tic_main)}")

    return df


# ------------------------------------------------------------------------------
# ------------------------- H A N D L E    D A T A -----------------------------
# ------------------------------------------------------------------------------


def handle_actual_arrival_date(
    df,
    col_ade="arrival_date_estimate",
    col_ada="arrival_date_actual",
    col_report_month="report_month",
):
    """Find out the rows where the year of actual arrival date
    significantly differs from the year of report. And replace them.

    Args:
        df (Pandas DataFrame): Dataframe w/ the estimated and actual date
        col_ada (str): column's name w/ actual arrival dates
        return_delay (bool): Either return the 'delay' column in DF
    Returns:
        Pandas DataFrame : Data combined into one DF
    """
    print(f"\nHandle the Actual Arrival Date .................................")
    tic = time.time()

    # * Handle the YEAR in the Actual Arrival Date
    mask_a = (abs(df[col_ada].dt.year - df[col_report_month].dt.year) > 1) & (
        abs(df[col_ada] - df[col_ade]).dt.days > 360
    )
    # View before processing
    print(f"Records w/ mistakes in the 'year' in actual arrival date:")
    print(f"\t# {sum(mask_a):,} ...")
    print(f"\t% {sum(mask_a)/len(df) * 100} ...")
    # Calc average delay
    avg = (df[mask_a][col_ada] - df[mask_a][col_ade]).dt.days.mean()
    print(f"Examples: Before: avg.delay = {avg} days.")
    display(df.loc[mask_a, [col_ada, col_ade, col_report_month]])
    # Replace YEAR in Actual with the YEAR of Estimated
    # TODO: Find more quikly way to replace year
    df[col_ada] = df[col_ada].mask(
        mask_a,
        pd.to_datetime(
            df[col_ade].dt.year.astype("str") + df[col_ada].dt.strftime("%m%d"),
            format="%Y%m%d",
        ),
    )
    # View results
    avg = (df[mask_a][col_ada] - df[mask_a][col_ade]).dt.days.mean()
    print(f"Examples: After processing: avg.delay = {avg} days.")
    display(df.loc[mask_a, [col_ada, col_ade, col_report_month]])

    # * Handle the YEAR in the Estimate Arrival Date
    mask_e = (abs(df[col_ade].dt.year - df[col_report_month].dt.year) > 1) & (
        abs(df[col_ada] - df[col_ade]).dt.days > 360
    )
    # View before processing
    print(f"Records w/ mistakes in the 'year' in estimate arrival date:")
    print(f"\t# {sum(mask_e):,} ...")
    print(f"\t% {sum(mask_e)/len(df) * 100} ...")
    # Calc average delay
    avg = (df[mask_e][col_ada] - df[mask_e][col_ade]).dt.days.mean()
    print(f"Examples: Before: avg.delay = {avg} days.")
    display(df.loc[mask_e, [col_ada, col_ade, col_report_month]])
    # Replace YEAR in Actual with the YEAR of Estimated
    # TODO: Find more quikly way to replace year
    df[col_ade] = df[col_ade].mask(
        mask_e,
        pd.to_datetime(
            df[col_ada].dt.year.astype("str") + df[col_ade].dt.strftime("%m%d"),
            format="%Y%m%d",
        ),
    )
    # View results
    avg = (df[mask_e][col_ada] - df[mask_e][col_ade]).dt.days.mean()
    print(f"Examples: After processing: avg.delay = {avg} days.")
    display(df.loc[mask_e, [col_ada, col_ade, col_report_month]])

    winsound.Beep(frequency=2000, duration=200)
    print(f"Handled the Actual Arrival Date {timing(tic)}")

    return df


def handle_estimated_arrival_date(
    df,
    col_ade="arrival_date_estimate",
    col_ada="arrival_date_actual",
    col_delay_name="arrival_date_delay",
    return_delay=True,
):
    """Find out the rows where the difference between
    Estimated Arrival Date and Actual Arrival Date is more then 360 days and
    replace the year in the 'estimated arrival' with year from the 'actual'.

    Args:
        df (Pandas DataFrame): Dataframe w/ the estimated and actual date
        col_ade (str): column's name w/ estimated arrival dates
        col_ada (str): column's name w/ actual arrival dates
        return_delay (bool): Either return the 'delay' column in DF
    Returns:
        Pandas DataFrame : Data combined into one DF
    """
    # col_ade = "estimate_arrival_date"
    # col_ada = "actual_arrival_date"
    print(f"\nHandle the Estimated Arrival Date ..............................")
    tic = time.time()

    # Calculate the delays
    df[col_delay_name] = (df[col_ada] - df[col_ade]).dt.days

    print(f"Handle the number-of-year mistakes ...............................")
    gap = 300

    # * Handle the records w/ dates passed through the New Year
    mask_ny_pos = (df[col_delay_name] > gap) & (
        (df[col_ada].dt.month == 1)
        | (df[col_ada].dt.month == 2)
        | (df[col_ada].dt.month == 3)
        | (df[col_ade].dt.month == 1)
        | (df[col_ade].dt.month == 2)
    )
    mask_ny_neg = (df[col_delay_name] < -gap) & (
        (df[col_ada].dt.month == 1)
        | (df[col_ada].dt.month == 2)
        | (df[col_ada].dt.month == 3)
        | (df[col_ade].dt.month == 1)
        | (df[col_ade].dt.month == 2)
    )
    print(f"Records w/ the dates passed through the New Year:")
    print(f"\t# {sum(mask_ny_pos) + sum(mask_ny_neg):,} ...")
    print(f"\t% {(sum(mask_ny_pos) + sum(mask_ny_neg))/len(df) * 100} ...")
    avg = df.loc[(mask_ny_pos | mask_ny_neg), col_delay_name].mean()
    print(f"Examples: Before: avg.delay = {avg} days.")
    display(df.loc[(mask_ny_pos | mask_ny_neg), [col_ada, col_ade, col_delay_name]])
    # Shift 1 YEAR to DOWN for Estimated Date for super EARLIER arrival
    df[col_ade] = df[col_ade].mask(
        mask_ny_neg,
        pd.to_datetime(
            df[col_ade] + pd.offsets.DateOffset(years=-1),
            format="%Y%m%d",
        ),
    )
    # Shift 1 YEAR to UP for Estimated Date for super LATER arrival
    df[col_ade] = df[col_ade].mask(
        mask_ny_pos,
        pd.to_datetime(
            df[col_ade] + pd.offsets.DateOffset(years=1),
            format="%Y%m%d",
        ),
    )
    # ReCalculate the delays
    df[col_delay_name] = (df[col_ada] - df[col_ade]).dt.days
    # View results
    avg = df.loc[(mask_ny_pos | mask_ny_neg), col_delay_name].mean()
    print(f"Examples: After processing: avg.delay = {avg} days.")
    display(df.loc[(mask_ny_pos | mask_ny_neg), [col_ada, col_ade, col_delay_name]])

    # * Handle the records w/ super later arrival
    mask_pos = df[col_delay_name] > gap
    # View before processing
    print(f"Records w/ the super LATER arrival. Days' gap > {gap} days:")
    print(f"\t# {sum(mask_pos):,} ...")
    print(f"\t% {sum(mask_pos)/len(df) * 100} ...")
    # Calc average delay
    avg = df.loc[mask_pos, col_delay_name].mean()
    print(f"Examples: Before: avg.delay = {avg} days.")
    display(df.loc[mask_pos, [col_ada, col_ade, col_delay_name]])
    # Shift 1 YEAR to UP for Estimated Date for super LATER arrival
    df[col_ade] = df[col_ade].mask(
        mask_pos,
        pd.to_datetime(
            df[col_ade] + pd.offsets.DateOffset(years=1),
            format="%Y%m%d",
        ),
    )
    # ReCalculate the delays
    df[col_delay_name] = (df[col_ada] - df[col_ade]).dt.days
    # View results
    avg = df.loc[mask_pos, col_delay_name].mean()
    print(f"Examples: After processing: avg.delay = {avg} days.")
    display(df.loc[mask_pos, [col_ada, col_ade, col_delay_name]])

    # * Handle the records w/ super later arrival
    mask_neg = df[col_delay_name] < -gap
    # View before processing
    print(f"Records w/ the super EARLIER arrival. Days' gap > -{gap} days:")
    print(f"\t# {sum(mask_neg):,} ...")
    print(f"\t% {sum(mask_neg)/len(df) * 100} ...")
    # Calc average delay
    avg = df.loc[mask_neg, col_delay_name].mean()
    print(f"Examples: Before: avg.delay = {avg} days.")
    display(df.loc[mask_neg, [col_ada, col_ade, col_delay_name]])
    # Shift 1 YEAR to DOWN for Estimated Date for super EARLIER arrival
    df[col_ade] = df[col_ade].mask(
        mask_neg,
        pd.to_datetime(
            df[col_ade] + pd.offsets.DateOffset(years=-1),
            format="%Y%m%d",
        ),
    )
    # ReCalculate the delays
    df[col_delay_name] = (df[col_ada] - df[col_ade]).dt.days
    # View results
    avg = df.loc[mask_neg, col_delay_name].mean()
    print(f"Examples: After processing: avg.delay = {avg} days.")
    display(df.loc[mask_neg, [col_ada, col_ade, col_delay_name]])

    if not return_delay:
        df.drop(columns=[col_delay_name])

    winsound.Beep(frequency=2000, duration=200)
    print(f"Handled the Estimated Arrival Date {timing(tic)}")

    return df


def handle_vessels(
    df,
    path_to_vessel_data,
    col_code="vessel_code",
    col_name="vessel_name",
    col_mode="mode_of_transportation",
    return_match_score=False,
    return_original_cols=False,
):

    print(f"\nHandle the Vessels .............................................")
    tic_main = time.time()
    SCORER_FIRST = fuzz.token_sort_ratio
    SCORER_SECOND = fuzz.token_set_ratio
    SCORE_CUTOFF_MATCH = 80
    SCORE_CUTOFF_MANUAL = 90

    df.rename(
        columns={
            col_code: "vessel_imo_bol",
            col_name: "vessel_name_bol",
        },
        inplace=True,
    )

    print(f"Handle Mode of Transportation ....................................")
    df["vessel_type_bol"] = [
        "container_ship" if x == 10 else "non_container_ship" for x in df[col_mode]
    ]
    df.drop(columns=[col_mode], inplace=True)

    print(f"Handle IMO-like names ............................................")
    # Separate IMO-like and names: Fill NA for using valid_int()
    df["vessel_imo_bol"] = [x if valid_int(x) else None for x in df["vessel_imo_bol"].fillna("N/A")]
    df["vessel_name_bol"] = [
        x if not valid_int(x) else None for x in df["vessel_name_bol"].fillna("N/A")
    ]

    # Fill NA for grouped data
    df.fillna(
        {"vessel_imo_bol": df.groupby("vessel_name_bol")["vessel_imo_bol"].transform("first")},
        inplace=True,
    )
    df.fillna(
        {"vessel_name_bol": df.groupby("vessel_imo_bol")["vessel_name_bol"].transform("first")},
        inplace=True,
    )

    num_vn_records_na = sum(df["vessel_name_bol"].isna())
    num_vn_records = sum(df["vessel_name_bol"].notna())
    num_vn_unique = len(df["vessel_name_bol"].dropna().unique())
    print(f"\nTotal records: # {len(df):,} records")
    print(f"\tVessels Names N/A records: # {num_vn_records_na:,}")
    print(f"\tVessels Names records: # {num_vn_records:,}")
    print(f"\tVessels Names uniques: # {num_vn_unique:,}")

    num_imo_records_na = sum(df["vessel_imo_bol"].isna())
    num_imo_records = sum(df["vessel_imo_bol"].notna())
    num_imo_unique = len(df["vessel_imo_bol"].dropna().unique())
    print(f"\n\tVessels IMO N/A records: # {num_imo_records_na:,}")
    print(f"\tVessels IMO records: # {num_imo_records:,}")
    print(f"\tVessels IMO uniques: # {num_imo_unique:,}")

    # df[df["vessel_name_bol"].str.contains("\d{7}", na=False, regex=True)]

    # **************************************************************************
    print(f"\nGet the data from Vessel Tracker data ..........................")
    df_vt = pd.read_csv(
        path_to_vessel_data,
        header=0,
        usecols=["vessel_name", "imo", "my_vessel_type.0"],
        dtype={"imo": "str", "mmsi": "str"},
    )
    df_vt.rename(
        columns={
            "vessel_name": "vessel_name",
            "imo": "vessel_imo",
            "my_vessel_type.0": "vessel_type",
        },
        inplace=True,
    )

    # Clear VT data from the cases when vessels' names has several IMOs
    df_vt = (
        df_vt.sort_values(["vessel_name"], na_position="last")
        .groupby(["vessel_name"], as_index=False)
        .first()
    )
    df_vt.info()

    # **************************************************************************
    print(f"\nMake merging for IMOs from the BoL stuff .......................")
    df = pd.merge(
        df,
        df_vt,
        how="left",
        left_on="vessel_imo_bol",
        right_on="vessel_imo",
    )

    # TODO: Possible should be reviewed
    df["vessel_match_score"] = [100 if isinstance(x, str) else 0 for x in df["vessel_name"]]

    # Print results
    matched_imo_records = len(df[df["vessel_imo_bol"].notna() & df["vessel_imo"].notna()])
    notmatched_imo_records = len(df[df["vessel_imo_bol"].notna() & df["vessel_imo"].isna()])

    matched_imo_unique = len(
        df[df["vessel_imo_bol"].notna() & df["vessel_name"].notna()]["vessel_imo_bol"].unique()
    )
    notmatched_imo_unique = len(
        df[df["vessel_imo_bol"].notna() & df["vessel_name"].isna()]["vessel_imo_bol"].unique()
    )

    prc_records = round(100 * matched_imo_records / num_imo_records, 2)
    prc_unique = round(100 * matched_imo_unique / num_imo_unique, 2)

    print(f"Vessels Names records (imo-like): # {num_imo_records:,}")
    print(f"\t# {matched_imo_records:,} got IMOs - % {prc_records}")
    print(f"\t# {notmatched_imo_records:,} w/o IMOs")
    print(f"Vessels Names unique (imo-like): # {num_imo_unique:,}")
    print(f"\t# {matched_imo_unique:,} got IMOs - % {prc_unique}")
    print(f"\t# {notmatched_imo_unique:,} w/o IMOs")

    # Get results
    list_notmatched_bol = (
        df[df["vessel_match_score"] == 0]["vessel_name_bol"].dropna().unique().tolist()
    )
    list_names_vt = df_vt["vessel_name"].to_list()

    # **************************************************************************
    ncores = mp.cpu_count()
    print(f"Number of cores in system: {ncores}")
    print(f"\nMake FIRST fuzzy matching for names from the BoL data ..........")
    print(f"\twith scorer: {SCORER_FIRST.__name__}")

    # # * With usual processing
    # list_matched_vn = do_usual_fuzzy_matching(
    #     names_from_bol_str,
    #     names_from_vt,
    # )

    # * With multi-processing
    # Freeze params and function as object
    make_fuzzy_matching_partial = partial(
        do_fuzzy_matching,
        list_correct_items=list_names_vt,
        scorer=SCORER_FIRST,
        score_cutoff=SCORE_CUTOFF_MATCH,
        verbose=False,
    )

    tic = time.time()
    list_matched_vn_1 = do_parallel_works_with_list(
        list_notmatched_bol,
        make_fuzzy_matching_partial,
        ncores,
    )
    print(f"Multiprocessing fuzzy matching {timing(tic)}")

    # Get results
    list_matched_bol_1 = [x[0] for x in list_matched_vn_1 if x[2] > SCORE_CUTOFF_MANUAL]
    list_notmatched_bol_1 = [x[0] for x in list_matched_vn_1 if x[2] <= SCORE_CUTOFF_MANUAL]
    prc_unique = round(100 * len(list_matched_bol_1) / len(list_notmatched_bol), 2)
    print(
        f"""
    First approach with scorer: {SCORER_FIRST.__name__}
    Have tried match for Vessels Names unique: # {len(list_notmatched_bol):,}
    \t# {len(list_matched_bol_1):,} got IMOs - % {prc_unique}
    \t# {len(list_notmatched_bol_1):,} w/o IMOs
    """
    )

    # **************************************************************************
    print(f"\nMake SECOND fuzzy matching for names from the BoL data .........")
    print(f"\twith scorer: {SCORER_SECOND.__name__}")
    # * freeze second params and function as object
    make_fuzzy_matching_partial = partial(
        do_fuzzy_matching,
        list_correct_items=list_names_vt,
        scorer=SCORER_SECOND,
        score_cutoff=SCORE_CUTOFF_MATCH,
        verbose=False,
    )

    tic = time.time()
    list_matched_vn_2 = do_parallel_works_with_list(
        list_notmatched_bol_1,
        make_fuzzy_matching_partial,
        ncores,
    )
    print(f"Multiprocessing fuzzy matching {timing(tic)}")

    # Get results
    list_matched_bol_2 = [x[0] for x in list_matched_vn_2 if x[2] != 0]
    list_notmatched_bol_2 = [x[0] for x in list_matched_vn_2 if x[2] == 0]

    prc_unique = round(100 * len(list_matched_bol_2) / len(list_notmatched_bol_1), 2)
    print(
        f"""
    Second approach: with scorer: {SCORER_SECOND.__name__}
    Rest of Vessels Names unique: # {len(list_notmatched_bol_1):,}
    \t# {len(list_matched_bol_2):,} got IMOs - % {prc_unique}
    \t# {len(list_notmatched_bol_2):,} w/o IMOs"""
    )

    # Join lists w/ results
    list_matched_vn = [x for x in list_matched_vn_1 if x[2] > SCORE_CUTOFF_MANUAL]
    list_matched_vn.extend(list_matched_vn_2)

    # **************************************************************************
    print(f"\nCreate DataFrame w/ matched names and IMOs .....................")
    # Create DF from list of results
    df_match = pd.DataFrame(
        list_matched_vn,
        columns=[
            "vessel_name_bol",
            "vessel_name",
            "vessel_match_score",
        ],
    )
    # Merge results with Vessel Tracker data
    df_match = pd.merge(
        df_match,
        df_vt,
        how="left",
        on="vessel_name",
    )

    # Split main DF onto fully matched & non-matched names
    df_100 = df[df["vessel_match_score"] == 100]
    df_000 = df[df["vessel_match_score"] == 0].drop(
        columns=[
            "vessel_name",
            "vessel_imo",
            "vessel_type",
            "vessel_match_score",
        ]
    )

    # Merge results with Vessel Tracker data
    df_000 = pd.merge(
        df_000,
        df_match,
        how="left",
        on="vessel_name_bol",
    )

    # Make final DF
    df = pd.concat([df_100, df_000])
    df["vessel_match_score"].fillna(0, inplace=True)

    print(f"Fill not matched fields w/ original data .........................")
    df.fillna(
        {
            "vessel_name": df["vessel_name_bol"],
            "vessle_imo": df["vessel_imo_bol"],
            "vessel_type": df["vessel_type_bol"],
        },
        inplace=True,
    )
    df.loc[df["vessel_name"].fillna("N/A").str.isdigit(), "vessel_name"] = np.nan
    df.loc[df["vessel_type"].fillna("N/A").str.isdigit(), "vessel_type"] = np.nan

    df["vessel_name"] = df["vessel_name"].str.upper()

    # Get final results
    not_matched_records = sum(df["vessel_imo"].isna())
    not_matched_unique = len(df[df["vessel_imo"].isna()]["vessel_name_bol"].unique())
    matched_records = sum(df["vessel_imo"].notna())
    matched_unique = len(df[df["vessel_imo"].notna()]["vessel_name_bol"].unique())
    prc_records = round(100 * matched_records / num_vn_records, 2)
    prc_unique = round(100 * matched_unique / num_vn_unique, 2)

    print(f"\nFinally total records: # {len(df):,}")
    print(f"\t# {matched_records:,} got IMOs - % {prc_records}")
    print(f"\t# {not_matched_records:,} w/o IMOs")
    print(f"\nFinally unique vessels names in BoL: # {num_vn_unique:,}")
    print(f"\t# {matched_unique:,} got IMOs - % {prc_unique}")
    print(f"\t# {not_matched_unique:,} w/o IMOs")

    if not return_match_score:
        df.drop(columns=["vessel_match_score"], inplace=True)

    if not return_original_cols:
        df.drop(
            columns=[
                "vessel_imo_bol",
                "vessel_name_bol",
                "vessel_type_bol",
            ],
            inplace=True,
        )

    df = df[sorted(df.columns)]
    winsound.Beep(frequency=2000, duration=200)
    print(f"Handled the Vessels {timing(tic_main)}")

    return df


def handle_ports(
    df,
    path_to_port_data,
    port_data_col_code="Port Code",
    port_data_cols_join=["lat", "lon", "country", "continent"],
    port_to_handle="port_of_lading",
):
    print(f"\nHandle the {port_to_handle.upper()}.............................")
    tic_main = time.time()
    col_code = f"{port_to_handle}_code"

    print(f"Get the Port data ................................................")
    df_pt = pd.read_csv(
        path_to_port_data,
        usecols=[port_data_col_code] + port_data_cols_join,
        dtype={port_data_col_code: "str"},
    )
    df_pt.rename(
        columns=dict(
            zip(
                [port_data_col_code] + port_data_cols_join,
                [col_code] + [port_to_handle + "_" + i for i in port_data_cols_join],
            )
        ),
        inplace=True,
    )
    # TODO: Normalize the Port data? pandas.Series.str.normalize
    # Clear Port data from the cases when port' name has several codes
    df_pt = (
        df_pt.sort_values([col_code], na_position="last")
        .groupby([col_code], as_index=False)
        .first()
    )
    # df_pt.info()

    print(f"Make merging for Port Codes ......................................")
    df = pd.merge(
        df,
        df_pt,
        how="left",
        left_on=col_code,
        right_on=col_code,
    )

    df = df[sorted(df.columns)]
    winsound.Beep(frequency=2000, duration=200)
    print(f"Handled the {port_to_handle.upper()} {timing(tic_main)}")

    return df


def split_column_by_pattern(
    df,
    col_to_split: str,
    first_col_name: str,
    second_col_name: str,
    pattert_for_split=",",
    fill_na=False,
    return_original_col=False,
):
    print(f"\nHandle the {col_to_split.upper()} ..............................")
    tic = time.time()

    df[[first_col_name, second_col_name]] = df[col_to_split].str.split(
        pat=pattert_for_split,
        n=1,
        expand=True,
    )
    # Remove punctuation
    df[second_col_name] = df[second_col_name].str.replace(r"[^\w\s]+", "", regex=True)

    if fill_na:
        df.fillna(
            {first_col_name: fill_na, second_col_name: fill_na},
            inplace=True,
        )

    if not return_original_col:
        df.drop(columns=[col_to_split], inplace=True)

    df = df[sorted(df.columns)]
    winsound.Beep(frequency=2000, duration=200)
    print(f"Handled the {col_to_split.upper()} {timing(tic)}")

    return df


def handle_listed_data(
    df,
    cols_to_handle: list,
):
    for col in cols_to_handle:
        print(f"\nHandle the {col.upper()} ...................................")
        tic = time.time()
        # Split the data, remove empty strings from resulting list and get uniques
        df[col] = (
            df[col]
            .fillna("N/A")
            .replace(r"\s+", "", regex=True)
            .str.split(pat=",", expand=False)
            .apply(lambda x: ", ".join(set(filter(None, x))))
        )
        df[col].replace({"N/A": np.nan}, inplace=True)
        winsound.Beep(frequency=2000, duration=200)
        print(f"Handled the {col.upper()} {timing(tic)}")

    return df


def handle_company(
    df,
    col_name: str,
    col_address: str,
    return_original_cols=False,
    **kwargs,
):
    """Group (deduplicating) strings for <NAME> column
    with following deduplicating for <ADDRESS> column

    Args:
        df (Pandas DF): DF with <NAME> & <ADDRESS> columns
        col_name (str): column's name for <NAME>
        col_address (str): column's name for <ADDRESS>
        return_original_cols (bool, optional): Defaults to False.
        **kwarg: kwarg for 'group_similar_strings' func
            see more: https://github.com/Bergvca/string_grouper
    Returns:
        Pandas DF: DF with grouped entities
    """

    MIN_SIMILARITY = 0.8
    col_name_grouped = f"{col_name}_grouped"
    col_address_grouped = f"{col_address}_grouped"

    print(f"\nHandle the {col_name.upper()} ..................................")
    tic_main = time.time()

    # Get info
    num_cn_records_na = sum(df[col_name].isna())
    num_cn_records = sum(df[col_name].notna())
    num_cn_dupes = sum(df[col_name].duplicated())
    num_cn_unique = len(df[col_name].dropna().unique())
    print(f"\nInitial dataset: # {len(df):,} records")
    print(f"\t[{col_name}] NA records: # {num_cn_records_na:,}")
    print(f"\t[{col_name}] records: # {num_cn_records:,}")
    print(f"\t[{col_name}] duplicates: # {num_cn_dupes:,}")
    print(f"\t[{col_name}] unique: # {num_cn_unique:,}")

    print(f"Strip & UPPER data ...")
    df[col_name] = df[col_name].str.strip().str.upper()
    df[col_address] = df[col_address].str.strip().str.upper()

    # Create DF to work for grouping
    _df = df[[col_name]].copy()

    # Drop full duplicates and NaNs
    dups = sum(_df.duplicated())
    nans = sum(_df[col_name].isna())

    _df.drop_duplicates(inplace=True)
    _df.dropna(inplace=True)

    print(f"\nHave been dropped NaNs # {nans:,}")
    print(f"Have been dropped duplicated # {dups:,}")
    print(f"Total rows to dedupe before preprocessing: # {len(_df):,}")

    # Preprocess data
    _df, long_words_idxs, processed_col_name = preprocess_column_to_group(
        _df,
        col=col_name,
        decode=True,
        puncts=True,
    )

    _df, _ = do_ngram_grouping(
        _df,
        n_gram=6,
        min_similarity=MIN_SIMILARITY,
        col_name=processed_col_name,
        **kwargs,
    )
    _df, _ = do_ngram_grouping(_df, 5, MIN_SIMILARITY, None, **kwargs)
    _df, _ = do_ngram_grouping(_df, 4, MIN_SIMILARITY, None, **kwargs)
    _df, _ = do_ngram_grouping(_df, 3, MIN_SIMILARITY, None, **kwargs)
    _df, cols_added = do_ngram_grouping(_df, 3, MIN_SIMILARITY, None, **kwargs)

    print(f"\nUnique '{col_name}' in: # {len(_df.iloc[:, 1].unique()):,}")
    print(f"Unique '{col_name_grouped}' out: # {len(_df.iloc[:, -1].unique()):,}")

    print(f"\nReturn the long words in their places ..........................")
    _df["group"] = _df[cols_added[1]].copy()
    for pair, idxs in long_words_idxs.items():
        # Get the group_id which was transformed through long words
        group_ids = _df.loc[idxs][cols_added[0]].unique().tolist()
        idxs_new = _df[_df[cols_added[0]].isin(group_ids)].index.tolist()

        print(f"{pair[1]} -> {pair[0]} : all # {len(idxs_new):,} \ grouped # {len(group_ids):,}")

        _df.loc[idxs_new, "group"] = _df.loc[idxs_new, "group"].str.replace(pair[1], pair[0])
    _df.rename(columns={"group": col_name_grouped}, inplace=True)
    winsound.Beep(frequency=2000, duration=200)

    print(f"\nMerge results with initial dataset .............................")
    df = pd.merge(
        df,
        _df[
            [
                col_name,
                col_name_grouped,
            ]
        ],
        how="left",
        left_on=col_name,
        right_on=col_name,
    )

    print(f"\nHandle {col_address} ...........................................")
    print(f"\tUnique '{col_address}' in: # {len(df[col_address].unique()):,}")
    df[col_address_grouped] = df.groupby([col_name_grouped])[col_address].transform("first")

    # Replace NA in grouped address with non-digit original address
    df.loc[
        np.logical_and(
            df[col_address_grouped].isna(),
            np.logical_not(df[col_address].str.isdigit()),
        ),
        col_address_grouped,
    ] = df[col_address]

    num_address = len(df[col_address_grouped].unique())
    print(f"\tUnique '{col_address_grouped}' out: # {num_address:,}")

    df = drop_duplicated(df)

    # Get final results
    num_gcn_dupes = sum(df[col_name_grouped].duplicated())
    num_gcn_unique = len(df[col_name_grouped].dropna().unique())

    print(f"\nBefore grouping: # {len(df):,} all records")
    print(f"\t[{col_name}] duplicates: # {num_cn_dupes:,}")
    print(f"\t[{col_name}] unique: # {num_cn_unique:,}")
    print(f"After grouping: # {len(df):,} all records")
    print(f"\tGrouped [{col_name}] duplicates: # {num_gcn_dupes:,}")
    print(f"\tGrouped [{col_name}] unique: # {num_gcn_unique:,}")

    if not return_original_cols:
        df.drop(columns=[col_name, col_address], inplace=True)
        df.rename(
            columns={
                col_name_grouped: col_name,
                col_address_grouped: col_address,
            },
            inplace=True,
        )

    df = df[sorted(df.columns)]
    winsound.Beep(frequency=2000, duration=200)
    print(f"Handled the {col_name.upper()} {timing(tic_main)}")

    return df


def handle_numeric_outliers(
    df,
    cols_to_handle=["teu", "quantity", "cif"],
    cols_name_suffix="outliers_off",
    outliers_treshold=0.99,
    return_original_cols=False,
):
    print(f"\nHandle the numeric column(s) ...................................")
    tic_main = time.time()

    # Fix the negative values
    df[cols_to_handle] = df[cols_to_handle].abs()

    # Get outliers' indexes & limits
    df_out, cutoffs = outliers_get_quantiles(
        df,
        cols_to_handle,
        treshold=outliers_treshold,
    )

    cols_lim = [f"{x}_{cols_name_suffix}" for x in cols_to_handle]

    print(f"\nLimit the outliers w/ tresholds ...")
    for i, col in enumerate(cols_to_handle):

        df[cols_lim[i]] = df[col].copy()
        # Limit the data
        df.loc[df_out[col], cols_lim[i]] = cutoffs[col]

    # # Checkout
    # df.loc[df_out.sum(axis=1) > 0, cols_to_handle + cols_lim]

    if not return_original_cols:
        df.drop(columns=cols_to_handle, inplace=True)
        df.rename(columns=dict(zip(cols_lim, cols_to_handle)), inplace=True)

    df = df[sorted(df.columns)]
    winsound.Beep(frequency=2000, duration=200)
    print(f"Handled the numeric column(s) {timing(tic_main)}")

    return df


def handle_weight_outliers(
    df,
    cols_to_handle=["weight_kg", "weight"],
    cols_name_suffix="outliers_off",
    outliers_treshold=0.99,
    return_original_cols=False,
):

    print(f"\nHandle the WEIGHTs column(s) ...................................")
    tic_main = time.time()

    # Fix the negative values
    df[cols_to_handle] = df[cols_to_handle].abs()

    # Get outliers' indexes & limits
    df_out, cutoffs = outliers_get_quantiles(
        df,
        cols_to_handle,
        treshold=outliers_treshold,
    )

    cutoff_wight = min(cutoffs.values())
    cols_lim = [f"{x}_{cols_name_suffix}" for x in cols_to_handle]

    print(f"\nLimit the outliers w/ tresholds ...")
    for i, col in enumerate(cols_to_handle):
        df[cols_lim[i]] = df[col].copy()
        # Limit the data
        df.loc[df_out[col], cols_lim[i]] = cutoff_wight

    # # Checkout
    # df.loc[df_out.sum(axis=1) > 0, cols_to_handle + cols_lim]

    if not return_original_cols:
        df.drop(columns=cols_to_handle, inplace=True)
        df.rename(columns=dict(zip(cols_lim, cols_to_handle)), inplace=True)

    df = df[sorted(df.columns)]
    winsound.Beep(frequency=2000, duration=200)
    print(f"Handled the WEIGHTs column(s) {timing(tic_main)}")

    return df


def handle_hscode(
    df,
    path_to_hscodes_table,
    col_to_handle="hscode",
    return_cargo_count=True,
):
    print(f"\nHandle the {col_to_handle.upper()} column(s) ...................")
    tic_main = time.time()

    # Split the data, remove empty strings from resulting list and get uniques
    df[col_to_handle] = (
        df[col_to_handle]
        .fillna("N/A")
        .replace(r"\s+", "", regex=True)
        .str.split(pat=",", expand=False)
        .apply(lambda x: ",".join(set(filter(None, x))))
    )
    df[col_to_handle].replace({"N/A": np.nan}, inplace=True)

    if return_cargo_count:
        df["cargo_count"] = (
            df[col_to_handle].replace(r"\s+", "", regex=True).str.split(",").str.len()
        )

    # Derive groups
    df["hscode_02"] = df[col_to_handle].str[:2]
    df["hscode_04"] = df[col_to_handle].str[:4]

    # * Get the HS Codes data
    df_hts = pd.read_csv(
        path_to_hscodes_table,
        # header=0,
        usecols=[
            "hscode_02_range",
            "hscode_02_desc_short",
            "hscode_04",
            "hscode_04_desc_short",
        ],
        dtype={"hscode_02": object, "hscode_04": object},
    )
    # Add the HS Codes data
    df = pd.merge(
        df,
        df_hts.drop(columns=["hscode_02_range"]),
        how="left",
        on="hscode_04",
    )

    print(f"Fix some problems w/ wrong 'hscode_04' ...........................")
    # Create dict w/ last range codes for replacing later
    last_codes = df_hts["hscode_02_range"].unique().tolist()
    last_codes = [x.split()[2] for x in last_codes]
    # Create list of lists w/ descriptions
    codes_list = df_hts[df_hts["hscode_04"].isin(last_codes)][
        ["hscode_04", "hscode_02_desc_short", "hscode_04_desc_short"]
    ].values.tolist()
    # Create dictionary w/ codes & descriptions
    codes_dict = dict(zip([x[0][:2] for x in codes_list], codes_list))

    # Replase wrong codes
    for k, v in codes_dict.items():
        print(f"{k}: {v[0]}: {v[1]}")
        df.loc[
            (df["hscode_02_desc_short"].isna()) & (df["hscode_02"] == k),
            ["hscode_04", "hscode_02_desc_short", "hscode_04_desc_short"],
        ] = (
            v[0],
            v[1],
            v[2],
        )

    df = df[sorted(df.columns)]
    winsound.Beep(frequency=2000, duration=200)
    print(f"Handled the {col_to_handle.upper()} {timing(tic_main)}")

    return df


def handle_description(
    df,
    col_to_handle="product_desc",
):
    print(f"\nHandle the {col_to_handle.upper()} column(s) ...................")
    tic_main = time.time()

    # Remove unprintable simbols
    df[col_to_handle] = (
        df[[col_to_handle]]
        .fillna("N/A")
        .applymap(lambda y: "".join(filter(lambda x: x in printable, y)))
    )

    # strip the HTML tags from a string and remove duplicated descriptions
    df[col_to_handle] = (
        df[col_to_handle]
        .fillna("N/A")
        .str.split(r"<.*?>", expand=False, regex=True)
        .apply(lambda x: "; ".join(set(filter(None, x))))
    )

    df[col_to_handle].replace({"N/A": np.nan}, inplace=True)

    df = df[sorted(df.columns)]
    winsound.Beep(frequency=2000, duration=200)
    print(f"Handled the {col_to_handle.upper()} {timing(tic_main)}")

    return df
