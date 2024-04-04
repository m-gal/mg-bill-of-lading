""" Contains the functions used in the project for Neo4j stuff for XPORTMINE data.

    @author: mikhail.galkin
"""

# %% Import needed python libraryies and project config info
import time
import winsound

import numpy as np
import pandas as pd

from IPython.display import display
from pprint import pprint

from mgbol.utils import timing
from mgbol.utils import drop_duplicated


# ------------------------------------------------------------------------------
# --------------- L O A D I N G   &   S A V I N G    S T U F F -----------------
# ------------------------------------------------------------------------------


def read_xport_processed_data(
    processed_folder_path,
    processed_files_names: list,  # or None
    cols_to_read: list,  # or None
    drop_dupes=True,
    **kwargs,
):
    """Read parquet-files w/ processed Xportmine BoL data
    and concatenate the data into one DataFrame

    Args:
        processed_folder_path (Path): Path to folder with data processed parquets.
        processed_files_names (list of str or None): files' names w/o extension.
        cols_to_read (list of str or None): Columns' names to read.
        drop_dupes (bool): Either check for duplicates
        **kwargs: kwargs for pandas.read_parquet()
    Returns:
        Pandas DataFrame : Data combined into one DF
    """

    print(f"\nRead the Xportmine processed data ..............................")
    tic_main = time.time()

    df = pd.DataFrame()  # empty DF for interim result

    print(f"---- Get processed data for the columns:")
    pprint("All columns..." if cols_to_read is None else cols_to_read)

    for file in processed_files_names:
        print(f"---- Read processed data from the <{file}> ...")

        _df = pd.read_parquet(
            path=processed_folder_path / f"{file}.parquet",
            columns=cols_to_read,
            engine="fastparquet",
        )

        # Concatenate the data
        df = pd.concat([df, _df], axis=0, ignore_index=True)
        del _df

    if drop_dupes:
        print(f"Dropping duplicates ...")
        df.drop_duplicates(inplace=True)

    print(f"\nLoaded dataset has # {len(df):,} records ...")
    # display(df.info(show_counts=True))
    winsound.Beep(frequency=2000, duration=200)
    print(f"All read {timing(tic_main)}")

    return df


# ------------------------------------------------------------------------------
# ------------------------- U T I L I T I E S ----------------------------------
# ------------------------------------------------------------------------------


def graph_from_cypher(graph):
    """Constructs a networkx graph from the results.graph() of a neo4j cypher query.
    Example of use:
    >>> result = session.run(query)
    >>> G = graph_from_cypher(result.graph())"""
    import networkx as nx

    G = nx.MultiDiGraph()

    nodes = list(graph._nodes.values())
    rels = list(graph._relationships.values())

    for node in nodes:
        G.add_node(
            node.id,
            labels=node._labels,
            properties=node._properties,
        )

    for rel in rels:
        G.add_edge(
            rel.start_node.id,
            rel.end_node.id,
            key=rel.id,
            type=rel.type,
            properties=rel._properties,
        )

    return G
