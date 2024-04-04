""" Contains sophisticated functions used for data processing

    @author: mikhail.galkin
"""

# %% Import needed python libraryies and project config info
import sys
import winsound
import time

import numpy as np
import pandas as pd

import nltk
from unidecode import unidecode
from string_grouper import group_similar_strings

import multiprocessing as mp
from tqdm import tqdm  # progress bar

# %% Load project's stuff -------------------------------------------------------
sys.path.extend([".", "./.", "././.", "..", "../..", "../../.."])

from mgbol.utils import timing

# ------------------------------------------------------------------------------
# ---------------------- F U Z Z Y   M A T C H I N G ---------------------------
# ------------------------------------------------------------------------------


def do_fuzzy_matching(
    list_wrong_items,
    list_correct_items,
    scorer=None,
    score_cutoff=80,
    verbose=True,
):
    """
    https://stackoverflow.com/questions/31806695/when-to-use-which-fuzz-function-to-compare-2-strings

    Args:
        list_wrong_items ([type]): [description]
        list_correct_items ([type]): [description]
        scorer ([type], optional): [description]. Defaults to None.
        score_cutoff (int, optional): [description]. Defaults to 80.
        verbose (bool, optional): [description]. Defaults to True.

    Returns:
        [type]: [description]
    """
    from fuzzywuzzy import fuzz
    from fuzzywuzzy import process

    scorers = [
        fuzz.ratio,
        fuzz.partial_ratio,  #! works bad
        fuzz.token_sort_ratio,
        fuzz.token_set_ratio,
        fuzz.partial_token_sort_ratio,  #! works bad
        fuzz.partial_token_set_ratio,  #! works bad
        fuzz.QRatio,
        fuzz.UQRatio,  #! works bad
        fuzz.WRatio,  #! works bad
        fuzz.UWRatio,  #! works bad
    ]
    num_of_wrong_items = len(list_wrong_items)
    list_matched_items = []
    for i, wrong_item in enumerate(list_wrong_items):
        if wrong_item in list_correct_items:
            match = (wrong_item, 100)
        else:
            match = process.extractOne(
                query=wrong_item,
                choices=list_correct_items,
                scorer=scorer,
                score_cutoff=score_cutoff,
            )
            if match is None:
                match = (np.nan, 0)

        list_matched_items.append((wrong_item, match[0], match[1]))

        if verbose:
            print(f"{i+1}/{num_of_wrong_items} - {wrong_item} : {match}")
    return list_matched_items


def do_parallel_works_with_list(list_to_process, func, ncores):
    # Split the list and progress bar
    list_splited = np.array_split(list_to_process, ncores)
    pbar = tqdm(list_splited, desc="Do multi-processing workings ...")
    pool = mp.Pool(ncores)
    list_processed = list(pool.map(func, pbar))
    list_processed = sum(list_processed, [])  # make list flatten
    pool.close()  # close out processes
    pool.join()  # join processes

    return list_processed


def do_parallel_works_with_df(df_to_process, func, ncores):
    # Split the df and progress bar
    df_splited = np.array_split(df_to_process, ncores)
    pool = mp.Pool(ncores)
    df_processed = pd.concat(pool.map(func, df_splited))
    pool.close()  # close out processes
    pool.join()  # join processes

    return df_processed


# ------------------------------------------------------------------------------
# ----------------------- D E D U P L I C A T I O N ----------------------------
# ------------------------------------------------------------------------------
STOP_WORDS = [
    "001162NF",
    "1ST NOFITY PARTY",
    "1ST NOTIFY ADDRESS",
    "A LA ORDEN DE",
    "A LA ORDEN",
    "ACTUAL BUYER",
    "ACTUAL CONSIGNEE",
    "ADDRESS",
    "AND ADDRESS",
    "AND BDP",
    "AND FULL ADDRESS",
    "AND NOTIFY",
    "AND OPENER",
    "AS ABOVE",
    "AS AGENT",
    "AS BELOW",
    "AS CONSIGNE",
    "AS CONSIGNEE",
    "BILL OF LADING",
    "C O COVERINGS",
    "C O TARGET",
    "CARE OF",
    "COMPANY NAME",
    "COMPLETE NAME",
    "CONSIGN TO",
    "CONSIGNACIONES",
    "CONSIGNADA",
    "CONSIGNADO",
    "CONSIGNADOS",
    "CONSIGNATARIO",
    "CONSIGNATOR",
    "CONSIGNED TO ORDER",
    "CONSIGNED TO",
    "CONSIGNED",
    "CONSIGNEE CO",
    "CONSIGNEE COMPANY NAME ADDRESS",
    "CONSIGNEE COMPANY NAME AND ADDRESS",
    "CONSIGNEE IS",
    "CONSIGNEE",
    "CONSIGNEES",
    "DELIVERY ADDRESS",
    "ENDORSED",
    "FACILITY ADDRESS",
    "FOR DELIVERY TO",
    "FOR DELIVERY",
    "FULL NAME",
    "GLOBAL ADDRESS",
    "IMPORTADORA Y EXPORTADORA",
    "IMPORTER",
    "INFO",
    "INTERNATIONAL INTERNATIONAL",
    "ISSUED OR",
    "ISSUED",
    "MADE OUT",
    "NAME",
    "NEGOTIABLE ONLY IF",
    "NEGOTIABLE",
    "NO EXISTE",
    "NON-NEGOTIABLE",
    "NOT NEGOTIABLE",
    "NOTIFY ADDRESS",
    "NOTIFY PARTY",
    "NOTIFY",
    "OF SHIPPER",
    "OF THE SHIPPER",
    "OLD COMPANY ADDRESS",
    "OLD MAIN ADDRESS",
    "ON BEHALF OF",
    "ONLY IF",
    "ONTO",
    "OR ORDER",
    "PARTY TO",
    "PAY TO",
    "PICKUP ADDRESS",
    "PVUNTO",
    "SAME AS",
    "SHIP TO",
    "SHIPPER",
    "SHIPPING ADDRESS",
    "SHOULD READ",
    "STORE ADDRESS",
    "STRUCTURED ADDRESS",
    "THE ORDER OF",
    "TO BE ACKNOWLEDGED",
    "TO ORDER OF",
    "TO ORDER",
    "TO THE ORDER OF INTERNATIONAL",
    "TO THE ORDER OF THE SHIPPER",
    "TO THE ORDER OF",
    "TO THE ORDER",
    "TRANSITOS",
    "TRUE CONSIGNEE",
    "ULTIMATE CONSIGNEE",
    "UNLESS",
    "UNTO",
    "VARIOUS CONSIGNEE",
    "VARIOUS CONSIGNEES",
]
LONG_WORDS = {
    "AMERICA": "AME",
    "AUTOMOTIVE": "AUT",
    "COMERCIALIZADORA": "COZ",
    "COMPANY": "CMP",
    "CONSTRUCTION": "CNS",
    "CORPORATION": "CRP",
    "DISTRIBUTION": "DBN",
    "DISTRIBUTORS": "DBS",
    "ELECTRONICS": "ELE",
    "ENGINEERING": "ENG",
    "ENTERPRISE": "ENT",
    "ENTERPRISES": "ENS",
    "EQUIPMENT": "EQU",
    "EXPEDITORS": "EXS",
    "EXPORT": "EXR",
    "FORWARDER": "FWR",
    "FORWARDERS": "FWS",
    "FORWARDING": "FWD",
    "FREIGHT": "FRG",
    "FURNITURE": "FRN",
    "GLOBAL": "GLB",
    "HOME FURNISHING": "HFU",
    "IMPORT": "IMR",
    "INDUSTRIA E COMERCIO": "IEC",
    "INDUSTRIAL": "IND",
    "INDUSTRIES": "INS",
    "INTERNATIONAL": "INTL",
    "LIMITED": "LTD",
    "LOGISTICS": "LGS",
    "MANUFACTURER": "MNF",
    "MANUFACTURERS": "MNS",
    "MANUFACTURING": "MNG",
    "PACKAGING": "PCK",
    "PHARMACEUTICALS": "PHR",
    "PRODUCTS": "PRD",
    "SA DE CV": "SDC",  # before use the regex to get 'SADECV'
    "SERVICES": "SRV",
    "SHIPPING": "SHP",
    "SOLUTIONS": "SLT",
    "TECHNOLOGIES": "TCS",
    "TECHNOLOGY": "TCY",
    "TRADING": "TRD",
    "TRANSPORTATION": "TRN",
    "WORLDWIDE": "WRL",
}


def preprocess_column_to_group(
    df,
    col,
    decode=True,
    puncts=True,
    stop_words=STOP_WORDS,
    long_words=LONG_WORDS,
):
    """Do preprocessing for string column before grouping the stings

    Args:
        df (DataFrame): DataFrame w/ columns to dedupe
        col (string): column's name to dedupe
        decode (bool, optional): Whether use unidecoding
        puncts (bool, optional): Whether remove punctuation
        spaces (bool, optional): Whether remove extra whitespaces
        stop_words (list of strings, optional): Whether remove stop words
        long_words (dict of strings, optional): Whether replace long words

    Returns:
        pd.Series: may be added to DF
    """
    print(f"\nDoing < {col.upper()} > columns preprocessing ..................")
    tic = time.time()

    # ^ DO NOT FORGET TO .strip().upper() PROCESSING BEFORE WILL BE MERGED
    print(f"\tUPPERCASE ...")
    df[col] = df[col].str.strip().str.upper()

    # Make a copy
    new = f"{col}_processed"
    df[new] = df[col].copy()

    # print(f"\tReplace NA's with 'UNKNOWN' ...")
    # df[new] = df[new].fillna(value="UNKNOWN")

    len_before = len(df)
    df.dropna(subset=[new], inplace=True)
    print(f"\tDropped the NA's before preprocessing: # {len_before-len(df):,}")

    len_before = len(df)
    df.drop(labels=df[df[col].str.isdigit()].index, inplace=True)
    print(f"\tDropped the digits: # {len_before-len(df):,}")

    print(f"\tRemove '2+' digits sequences and 'C/O' marks ...")
    df[new] = df[new].str.replace(r"\d{2,}", " ", regex=True)
    df[new] = df[new].str.replace(r"(C\W{1}O)", " ", regex=True)

    print(f"\tReplace 'SA.DE.CV' & 'L.L.C' for further processing ...")
    df[new] = df[new].str.replace(
        r"(S\W?A\W*DE\W?C\W?V)",
        " SA DE CV ",
        regex=True,
    )
    df[new] = df[new].str.replace(r"(\W+L\W?L\W?C)", " LLC ", regex=True)

    print(f"\tReplace 'U.S.A' & parenthesis for further processing ...")
    df[new] = df[new].str.replace(r"(\W+U\W?S\W?A)", " USA ", regex=True)
    df[new] = df[new].str.replace(r"(\(.*\))", " ", regex=True)

    if decode:
        print(f"\tUnidecode ...")
        df[new] = df[new].apply(unidecode)

    if puncts:
        print(f"\tReplace '&' and '+' with AND ...")
        df[new] = df[new].str.replace("&", " AND ", regex=False)
        df[new] = df[new].str.replace("+", " AND ", regex=False)
        print(f"\tRemove punctuations ...")
        df[new] = df[new].str.replace(r"[^0-9A-Z]", " ", regex=True)

    print(f"\tRemove extra whitespaces before removing stop words ...")
    df[new] = df[new].str.replace(r"\s+", " ", regex=True)

    if stop_words is not None:
        print(f"\tRemove STUB stop words ...")
        pattern = "|".join(stop_words)
        df[new] = df[new].str.replace(pattern, " ", regex=False)

        print(f"\tRemove NLTK stop words ...")
        # Get list of stopwords
        nltk.download("stopwords")
        nltk_stop_words = nltk.corpus.stopwords.words("english")
        # Remove stop words
        df[new] = df[new].apply(
            lambda x: " ".join(w for w in x.split() if w not in nltk_stop_words)
        )

    if long_words is not None:
        long_words_idxs = {}
        print(f"\tReplace STUB long words ...")
        for k, v in long_words.items():
            # save indecies
            idxs = df[df[new].str.contains(k)].index.to_list()
            long_words_idxs[(k, v)] = idxs

            # replace
            df[new] = df[new].str.replace(k, v)

    print(f"\tRemove extra whitespaces after removing stop words ...")
    df[new] = df[new].str.replace(r"\s+", " ", regex=True)

    print(f"\tReplace empty strings & UNKNOWN with the None ...")
    df[new] = df[new].replace(r"^\s*$", None, regex=True)
    df[new] = df[new].replace("UNKNOWN", None, regex=False)

    len_before = len(df)
    df.dropna(subset=[new], inplace=True)
    print(f"\tDropped the NA's after preprocessing: # {len_before-len(df):,}")

    winsound.Beep(frequency=2000, duration=200)
    print(f"\nThe < {col.upper()} > : # {len(df):,} values was preprocessed.")
    print(f"Preprocessing {timing(tic)}")
    processed_col_name = new

    return df, long_words_idxs, processed_col_name


def do_ngram_grouping(df, n_gram, min_similarity, col_name=None, **kwargs):
    """
    For the group_similar_strings():
        :param ngram_size: int.
            The amount of characters in each n-gram.
            Default is 3.
        :param regex: str.
            The regex string used to cleanup the input string.
            Default is '[,-./]|\s'.
        :param max_n_matches: int.
            The maximum number of matching strings in master allowed per string in duplicates.
            Default is the total number of strings in master.
        :param min_similarity: float.
            The minimum cosine similarity for two strings to be considered a match.
            Defaults to 0.8.
        :param ignore_case: bool.
            Whether or not case should be ignored.
            Defaults to True (ignore case).
        :param ignore_index: bool.
            whether or not to exclude string Series index-columns in output.
            Defaults to False.
        :param include_zeroes: bool.
            when the minimum cosine similarity <=0, determines whether zero-similarity matches
            appear in the output.
            Defaults to True.
        :param replace_na:
            whether or not to replace NaN values in most similar string index-columns with
            corresponding duplicates-index values.
            Defaults to False.
        :param group_rep: str.
            The scheme to select the group-representative.  Default is 'centroid'.
            The other choice is 'first'.
        :param n_blocks: (int, int)
        This parameter is provided to help boost performance, if possible, of
        processing large DataFrames, by splitting the string Series into n_blocks[0] blocks
        for the left operand (of the underlying matrix multiplication) and into
        n_blocks[1] blocks for the right operand before performing the string-comparisons
        block-wise.
        Defaults to 'guess', in which case the numbers of blocks are estimated based
        on previous empirical results.  If n_blocks = 'auto', then splitting is done
        automatically in the event of an OverflowError.
    """
    if col_name is None:
        col_name = f"group_{n_gram+1}ng"

    tic = time.time()
    print(f"\nStart grouping entities w/ NGRAM = {n_gram} ----------------")
    cols_added = [f"group_id_{n_gram}ng", f"group_{n_gram}ng"]
    df[cols_added] = group_similar_strings(
        strings_to_group=df[col_name],
        ngram_size=n_gram,
        min_similarity=min_similarity,
        group_rep="centroid",
        **kwargs,
    )
    print(f"\nNGRAM = {n_gram} grouping {timing(tic)} ...")
    winsound.Beep(frequency=2000, duration=200)

    return df, cols_added


def drop_mismatches_id_name(df, col_id, col_name):
    """Find out and drop the inconsistent data when the same 'identifier'
        have a 2 or more different entities, e.x. Consignee_name

    Args:
        df (_type_): dataframe to check
        col_id (_type_): column's name which have to be unique
        col_name (_type_): column's name which could be different

    Returns:
        Cleaned DataFrame
    """

    # Get mask for groupedby 'col_id' where number of unique 'col_name' > 1
    mask = df.groupby(by=[col_id], dropna=False)[col_name].nunique(dropna=False) > 1

    print(f"#{sum(mask): ,} cases when for 1 '{col_id}' are 2 different '{col_name}'")
    # Get list of inconsistent identifiers
    bad_identifiers = mask[mask].index.to_list()
    # Move bad data to the temp DF
    df_temp = df[df[col_id].isin(bad_identifiers)].reset_index()
    # Get count of NA in each row
    df_temp["count_na"] = df_temp.isna().sum(axis=1)
    # Get rows' indexes with the minimal NA's
    idxs_min = df_temp.groupby(col_id)["count_na"].idxmin().values
    # Get original indexes to keep
    idxs_keep = df_temp.loc[idxs_min]["index"].to_list()
    # Get original indexes to drop
    idxs = df_temp["index"].to_list()
    idxs_drop = list(set(idxs) - set(idxs_keep))
    print(f"Will drop #{len(idxs_drop): ,} inconsistent rows...")
    df.drop(index=idxs_drop, inplace=True)

    return df
