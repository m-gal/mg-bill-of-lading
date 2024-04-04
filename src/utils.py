""" Contains the functions used across the project.

    @author: mikhail.galkin
"""

#%% Import needed python libraryies and project config info
import time
import numpy as np
import pandas as pd
import geopandas as gpd
from sklearn import metrics

from IPython.display import display
from pprint import pprint

import multiprocessing as mp
from tqdm import tqdm  # progress bar


# ------------------------------------------------------------------------------
# ----------------------------- P A R A M E T E R S ----------------------------
# ------------------------------------------------------------------------------
#%% Set up: Pandas options
def pd_set_options():
    """Set parameters for PANDAS to InteractiveWindow"""

    display_settings = {
        "max_columns": 40,
        "max_rows": 400,  # default: 60
        "width": 500,
        "max_info_columns": 500,
        "expand_frame_repr": True,  # Wrap to multiple pages
        "float_format": lambda x: "%.5f" % x,
        "pprint_nest_depth": 4,
        "precision": 4,
        "show_dimensions": True,
    }
    print("\nPandas options established are:")
    for op, value in display_settings.items():
        pd.set_option(f"display.{op}", value)
        option = pd.get_option(f"display.{op}")
        print(f"\tdisplay.{op}: {option}")


#%% Set up: Reset Pandas options
def pd_reset_options():
    """Set parameters for PANDAS to InteractiveWindow"""
    pd.reset_option("all")
    print("Pandas all options re-established.")


#%% Set up: Matplotlib params
def matlotlib_set_params():
    """Set parameters for MATPLOTLIB to InteractiveWindow"""
    import matplotlib.pyplot as plt
    from matplotlib import rcParams

    plt.style.use(["ggplot"])
    rcParams["figure.figsize"] = 10, 8
    rcParams["axes.spines.top"] = False
    rcParams["axes.spines.right"] = False
    rcParams["xtick.labelsize"] = 12
    rcParams["ytick.labelsize"] = 12


# ------------------------------------------------------------------------------
# --------------- L O A D I N G   &   S A V I N G    S T U F F -----------------
# ------------------------------------------------------------------------------
def save_df_as_parquet(df, dir_to_save, file_to_save):
    print(f"\nSave the results as <{file_to_save}.parquet> ...")
    tic = time.time()
    df.to_parquet(
        dir_to_save / f"{file_to_save}.parquet",
        index=False,
    )
    print(f"<{file_to_save}.parquet> have been saved {timing(tic)}")

    print(f"\nLoad: <{file_to_save}.parquet> to check identity...")
    tic = time.time()
    df_pq = pd.read_parquet(
        dir_to_save / f"{file_to_save}.parquet",
    )
    print(f"<{file_to_save}.parquet> have been loaded {timing(tic)}")

    print(f"\nCheck out the data ...")
    print(f"Are the datasets equal: {df.equals(df_pq)}")

    del df_pq


def save_gdf_as_parquet(gdf, dir_to_save, file_to_save):
    print(f"\nSave the GeoDataFrame as <{file_to_save}.parquet> ...")
    tic = time.time()
    gdf.to_parquet(
        dir_to_save / f"{file_to_save}.parquet",
        index=False,
    )
    print(f"<{file_to_save}.parquet> have been saved {timing(tic)}")

    print(f"\nLoad GeoDataFrame: <{file_to_save}.parquet> to check identity...")
    tic = time.time()
    gdf_pq = gpd.read_parquet(
        dir_to_save / f"{file_to_save}.parquet",
    )
    print(f"<{file_to_save}.parquet> have been loaded {timing(tic)}")
    print(f"{gdf_pq.info()}")

    print(f"\nCheck out the geo data ...")
    print(f"Are the Geo dataframes equal: {gdf.equals(gdf_pq)}")
    del gdf_pq


# ------------------------------------------------------------------------------
# ----------------------------- M E T R I C S ----------------------------------
# ------------------------------------------------------------------------------
def calc_mape(y_true, y_pred):
    """Mean absolute percentage error regression loss.

    Examples
    --------
    >>> from sklearn.metrics import mean_absolute_percentage_error
    >>> y_true = np.array([3, -0.5, 2, 7])
    >>> y_pred = np.array([2.5, 0.0, 2, 8])
    0.3273809523809524...

    >>> y_true = np.array([1.0, -1.0, 2, -2])
    >>> y_pred = np.array([0.7, -0.7, 1.4, -1.4])
    >>> mean_absolute_percentage_error(y_true, y_pred)
    0.30000000000000004...
    """
    # Epsilon: is an arbitrary small yet strictly positive numbe
    # to avoid undefined results when y is zero
    epsilon = np.finfo(np.float64).eps
    ape = np.abs(y_pred - y_true) / np.maximum(np.abs(y_true), epsilon)
    return np.average(ape)


def calc_binary_class_metrics(model, X, y, set_name=None):
    """https://neptune.ai/blog/evaluation-metrics-binary-classification"""
    print(f"\nCalculate model metrics...")
    model_name = type(model).__name__
    target = y.name
    y_pred = model.predict(X)
    y_pred_proba = model.predict_proba(X)[:, 1]

    if set_name is None:
        set_name = ""
    else:
        set_name = f"-{set_name}"
    # * Metrics
    cm = metrics.confusion_matrix(y, y_pred)
    tn, fp, fn, tp = cm.ravel()

    auc = metrics.roc_auc_score(y, y_pred)
    acc = (tp + tn) / (tp + tn + fp + fn)
    recall = tp / (tp + fn)  # True Positive Rate, Sensitivity
    precision = tp / (tp + fp)  # Positive Predictive Value
    f1 = (2 * recall * precision) / (recall + precision)
    avg_prec = metrics.average_precision_score(y, y_pred)
    log_loss = metrics.log_loss(y, y_pred)
    """Brier score is a measure of how far your predictions lie from the true values"""
    brier = metrics.brier_score_loss(y, y_pred_proba)
    """Cohen Kappa Metric tells you how much better is your model over the
    random classifier that predicts based on class frequencies."""
    kappa = metrics.cohen_kappa_score(y, y_pred)
    """Matthews Correlation Coefficient MCC It is a correlation between
    predicted classes and ground truth"""
    mcc = metrics.matthews_corrcoef(y, y_pred)
    fp_rate = fp / (fp + tn)  # False Positive Rate, Type I error
    fn_rate = fn / (tp + fn)  # False Negative Rate, Type II error
    tn_rate = tn / (tn + fp)  # True Negative Rate, Specificity
    neg_pv = tn / (tn + fn)  # Negative Predictive Value
    fal_dr = fp / (tp + fp)  # False Discovery Rate

    print(
        f"On {set_name} set the {model_name} for [{target}] gives following metrics:"
    )
    print(f"\tAUC:: {auc:.4f}")
    print(f"\tAccuracy:: {acc:.4f}")
    print(f"\tRecall:: {recall:.4f}")
    print(f"\tPrecision:: {precision:.4f}")
    print(f"\tF1:: {f1:.4f}")
    print(f"\tAverage precision:: {avg_prec:.4f}")
    print(f"\tLog loss:: {log_loss:.4f}")
    print(f"\tBrier score:: {brier:.4f}")
    print(f"\tCohen Kappa metric:: {kappa:.4f}")
    print(f"\tMatthews Correlation Coefficient:: {mcc:.4f}")

    eval_scores = {
        f"auc{set_name}": auc,
        f"acc{set_name}": acc,
        f"recall{set_name}": recall,
        f"precision{set_name}": precision,
        f"f1{set_name}": f1,
        f"avg_precision{set_name}": avg_prec,
        f"log_loss{set_name}": log_loss,
        f"brier{set_name}": brier,
        f"kappa{set_name}": kappa,
        f"mcc{set_name}": mcc,
        f"cm_tn{set_name}": tn,
        f"cm_fp{set_name}": fp,
        f"cm_fn{set_name}": fn,
        f"cm_tp{set_name}": tp,
        f"cm_fp_rate{set_name}": fp_rate,
        f"cm_fn_rate{set_name}": fn_rate,
        f"cm_tn_rate{set_name}": tn_rate,
        f"neg_pv{set_name}": neg_pv,
        f"false_dr{set_name}": fal_dr,
    }
    return eval_scores


# ------------------------------------------------------------------------------
# --------------------------- E X P L O R E ------------------------------------
# ------------------------------------------------------------------------------
def cols_get_na(df):
    """Get: Info about NA's in df"""
    print(
        f"\n#NA = {df.isna().sum().sum()}\n%NA = {df.isna().sum().sum()/df.size*100}"
    )
    # View NA's through variables
    df_na = pd.concat(
        [df.isna().sum(), df.isna().sum() / len(df) * 100, df.notna().sum()],
        axis=1,
        keys=["# NA", "% NA", "# ~NA"],
    ).sort_values("% NA")
    return df_na


def df_get_glimpse(df, n_rows=4):
    """Returns a glimpse for related DF

    Args:
        df ([type]): original Pandas DataFrame
        rnd_n_rows (int, optional): # randomly picked up rows to show.
            Defaults to 4.
    """
    #%% Get first info about data set
    print(f"\nGET GLIMPSE: ---------------------------------------------------")
    print(f"Count of duplicated rows : {df.duplicated().sum()}")
    print(f"\n----DF: Information about:")
    display(df.info(verbose=True, show_counts=True, memory_usage=True))
    print(f"\n----DF: Descriptive statistics:")
    display(df.describe(include=None).round(3).T)
    print(f"\n----DF: %Missing data:")
    display(cols_get_na(df))
    if n_rows is not None:
        print(f"\n----DF: Random {n_rows} rows:")
        display(df.sample(n=n_rows).T)


# ------------------------------------------------------------------------------
# -----------------C O L U M N S   M A N I P U L A T I O N S -------------------
# ------------------------------------------------------------------------------
def convert_to_datetime_inplace(df):
    """Automatically detect and convert (in place!) each
    dataframe column of datatype 'object' to a datetime just
    when ALL of its non-NaN values can be successfully parsed
    by pd.to_datetime().
    """
    from pandas.errors import ParserError

    for col in df.columns[df.dtypes == "object"]:
        try:
            df[col] = pd.to_datetime(df[col])
        except (ParserError, ValueError):
            pass
    return df


def cols_get_mixed_dtypes(df):
    print(f"Get info about columns w/ mixed types...")
    mixed_dtypes = {
        c: dtype
        for c in df.columns
        if (dtype := pd.api.types.infer_dtype(df[c])).startswith("mixed")
    }
    pprint(mixed_dtypes)
    mixed_dtypes = list(mixed_dtypes.keys())
    return mixed_dtypes


def drop_duplicated(df, subset=None):
    dupes = df.duplicated(subset).sum()
    if dupes == 0:
        print(f"Data has no duplicated records ...")
    else:
        print(f"Data has # {dupes} duplicated records ...")
        print(f"\t % {dupes/len(df)*100} of duplicated records ...")
        df.drop_duplicates(subset, keep="first", inplace=True)
        print(f"Dropped # {dupes:,} of duplicated records.")
    return df


def cols_reorder(df):
    cols = df.columns.to_list()
    cols.sort()
    df = df[cols]
    return df


def cols_coerce_to_num(df, cols_to_num):
    if cols_to_num is None:
        pass
    else:
        print(f"Coerce 'mixed' type column to numeric...")
        for col in cols_to_num:
            print(f"\tCoerse to numeric: {col}")
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def cols_coerce_to_str(df, cols_to_str, as_int=False):
    if cols_to_str is None:
        pass
    else:
        print(f"Coerce 'mixed' type column to string...")
        for col in cols_to_str:
            print(f"\tCoerse to string: {col}")
            if as_int and df[col].dtype == "float64":
                df[col] = df[col].astype(pd.Int64Dtype()).astype(str)
                df[col] = df[col].replace({"<NA>": np.nan})
            else:
                df[col] = df[col].astype(str)
                df[col] = df[col].replace({"nan": np.nan})
    return df


def cols_coerce_to_datetime(df, cols_to_datetime):
    if cols_to_datetime is None:
        pass
    else:
        print(f"Coerce 'mixed' type column to datetime...")
        for col in cols_to_datetime:
            print(f"\tCoerse to datetime: {col}")
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def cols_coerce_to_date(df, cols_to_date):
    if cols_to_date is None:
        pass
    else:
        print(f"Coerce 'mixed' type column to date...")
        for col in cols_to_date:
            print(f"\tCoerse to date: {col}")
            df[col] = df[col].dt.date
    return df


def cols_cat_to_dummies(df, cols_to_dummies):
    print(f"\nConvert categorical to pd.dummies (OHE)...")
    for col in cols_to_dummies:
        print(f"\tConvert column: {col}")
        dummy = pd.get_dummies(df[[col]])
        df = df.drop(columns=[col])
        df = df.merge(dummy, left_index=True, right_index=True)
    return df


def cols_coerce_all_to_dtypes(df):
    cols_int = df.select_dtypes(
        include=["Int32", "Int64"],
    ).columns.to_list()
    cols_float = df.select_dtypes(
        include=["Float32", "Float64"],
    ).columns.to_list()
    cols_string = df.select_dtypes(
        include=["string"],
    ).columns.to_list()

    print(f"Before:\n{df.info()}")

    # Convert float:
    if len(cols_string) != 0:
        for col in cols_string:
            try:
                df[col] = df[col].astype(str)
                print(f"<{col}> was converted to <{df[col].dtype}> ...")
            except Exception as e:
                print(f"... <{col}> {e} ...")

    # Convert Int
    if len(cols_int) != 0:
        for col in cols_int:
            try:
                df[col] = df[col].astype(int)
                print(f"<{col}> was converted to <{df[col].dtype}> ...")
            except Exception as e:
                print(f"... <{col}> {e}, will be converted to <float> ..")
                df[col] = df[col].astype(float)

    # Convert float:
    if len(cols_float) != 0:
        for col in cols_float:
            try:
                df[col] = df[col].astype(float)
                print(f"<{col}> was converted to <{df[col].dtype}> ...")
            except Exception as e:
                print(f"... <{col}> {e} ...")

    print(f"After:\n{df.info()}")

    return df


# ------------------------------------------------------------------------------
# -------------------------- U T I L I T I E S ---------------------------------
# ------------------------------------------------------------------------------
def timing(tic):
    min, sec = divmod(time.time() - tic, 60)
    return f"for: {int(min)}min {int(sec)}sec"


# ------------------------------------------------------------------------------
# --------------------------- O U T L I E R S ----------------------------------
# ------------------------------------------------------------------------------


def outliers_get_zscores(df, cols_to_check=None, sigma=3):
    print(f"\nGet columns w/ outliers w/ {sigma}-sigma...")
    cols_to_drop = ["sic", "cik", "countryba", "name", "ddate"]
    if cols_to_check is None:
        cols_to_check = df.columns.drop(cols_to_drop).tolist()
    else:
        cols_to_check = cols_to_check
    cols = []
    nums = []
    df_out = None
    for col in cols_to_check:
        mean = df[col].mean()
        std = df[col].std()
        z = np.abs(df[col] - mean) > (sigma * std)
        num_outs = z.sum()

        if num_outs > 0:
            print(f"\t{col}: {num_outs} ouliers.")
            display(df.loc[z, col])
            cols.append(col)
            nums.append(num_outs)
            df_out = pd.concat([df_out, z], axis=1)
    display(df_out.sum())
    return df_out


def outliers_get_quantiles(df, cols_to_check=None, treshold=0.999):
    print(f"\nGet columns w/ outliers w/ {treshold} quantile treshold...")
    if cols_to_check is None and "imo" in df.columns:
        cols_to_check = df.columns.drop("imo").tolist()
    else:
        cols_to_check = cols_to_check
    cols = []
    nums = []
    cutoffs = {}
    df_out = pd.DataFrame()
    for col in cols_to_check:
        cutoff = df[col].quantile(treshold)
        q = df[col] > cutoff
        num_outs = q.sum()
        prc_outs = num_outs / sum(df[col].notna()) * 100

        if num_outs > 0:
            print(
                f"\t{col.upper()}: cutoff = {cutoff}: # {num_outs:,} ouliers: % {prc_outs}"
            )
            cutoffs[col] = cutoff
            # display(df.loc[q, col])
            cols.append(col)
            nums.append(num_outs)
            df_out = pd.concat([df_out, q], axis=1)

    # display(df_out.sum())
    return df_out, cutoffs


def outliers_ridoff(df, df_out):
    print(f"\nRid off outliers ...")
    idx = df_out.sum(axis=1) > 0
    df = df[~idx]
    print(f"Totally deleted {sum(idx)} outliers...")
    print(f"Data w/o outliers has: {len(df)} rows X {len(df.columns)} cols")
    return df
