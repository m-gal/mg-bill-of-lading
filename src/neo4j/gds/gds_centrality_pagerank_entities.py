"""
    Apply Graph Data Science library
    for CENTRALITY PAGERANK algorithm
    to the Neo4j BoL GraphDB through the GDS python client
    @author: mikhail.galkin 2022
"""

# %% Setup ----------------------------------------------------------------------
from IPython.display import display
from graphdatascience import GraphDataScience
from tqdm import tqdm

# %% Load project's stuff -------------------------------------------------------
from mgbol.neo4j.neo_driver import neo4j_uri
from mgbol.neo4j.neo_driver import neo4j_user
from mgbol.neo4j.neo_driver import neo4j_pass


# %% MAIN -----------------------------------------------------------------------
def main():

    GRAPH_NAME = "mg-bol-pagerank"
    NODES_SPEC = {
        "Shipper": {"properties": ["_leid_id"]},
        "Consignee": {"properties": ["_leid_id"]},
        "NotifyParty": {"properties": ["_leid_id"]},
    }
    RELS_SPEC = {
        "SHIPS_FOR": {
            "orientation": "UNDIRECTED",
            "properties": {"delay_days_q95i": {"property": "delay_days_q95i"}},
        },
        "NOTIFY_FOR": {
            "orientation": "UNDIRECTED",
            "properties": {"delay_days_q95i": {"property": "delay_days_q95i"}},
        },
    }
    PROP_NAME = {
        "pagerank": "_pagerank",
        "pagerank_scaled": "_pr_scaled",
        "pagerank_weighted": "_pr_weighted",
        "pagerank_weighted_scaled": "_pr_wescaled",
        "leid": "_leid_id",
        "leid_size": "_leid_size",
        "leid_pagerank_avg": "_leid_prw_avg",
    }

    result = {}

    # Follow the convention to name `GraphDataScience` object as `gds`
    gds = GraphDataScience(neo4j_uri, auth=(neo4j_user, neo4j_pass))
    print(f"GDS version: {gds.version()}")

    # Check & drop all graphs in memory
    print(f"Check graphs in memory ...")
    graphs = gds.graph.list()["graphName"].to_list()
    if len(graphs) > 0:
        for graph in graphs:
            print(f"Dropped '{graph}' graph ...")
            G = gds.graph.get(graph)
            G.drop()
    else:
        print(f"No one projected graph in memory ...")

    print(f"Create the new projected graph '{GRAPH_NAME}' ...")
    # Where G is a Graph object, and res is a pd.Series containing metadata
    G, res = gds.graph.project(
        GRAPH_NAME,
        NODES_SPEC,
        RELS_SPEC,
    )
    result[GRAPH_NAME] = res
    display(result[GRAPH_NAME])

    # C E N T R A L I T Y :-----------------------------------------------------
    # * PAGERANK ---------------------------------------------------------------
    """
    (Weekly or Strongly) Connected Components are very useful algorithms for
    understanding a graph structure.
    ! PageRank can lead to unexpected result on graphs with multiple components.
    It is a good practice to run PageRank separately on each component.

    The WCC algorithm finds sets of connected nodes in an undirected graph,
    where all nodes in the same set form a connected component.
    WCC is often used early in an analysis to understand the structure of a graph.
    Using WCC to understand the graph structure enables running other algorithms
    independently on an identified cluster.
    As a preprocessing step for directed graphs, it helps quickly identify
    disconnected groups.
    """
    print(f"Derive existing Leiden communities IDs and its size ...")
    query_string = (
        f"MATCH (n) "
        f"WHERE n.{PROP_NAME['leid']} IS NOT NULL "
        f"RETURN n.{PROP_NAME['leid']} as leid_id, count(n) AS size "
        f"ORDER BY size DESC"
    )
    df = gds.run_cypher(query_string)
    print(f"There are {len(df): ,} Leiden communities ...")
    display(df.head(14))

    # Filter the df
    df0 = df[df["size"] != 1]
    result["leid"] = dict(zip(df0["leid_id"].values, df0["size"].values))

    df1 = df[df["size"] == 1]
    result["leid_empty"] = df1["leid_id"].to_list()
    print(f"Make '{PROP_NAME['pagerank']}' = 0 for empty communities ...")
    query_string = (
        f"MATCH (n) "
        f"WHERE n.{PROP_NAME['leid']} IN {result['leid_empty']} "
        f"WITH collect(n) AS nodes "
        f"FOREACH(n IN nodes | SET n.{PROP_NAME['pagerank']}=0)"
    )
    gds.run_cypher(query_string)

    # Filtered graph spec
    subgraph_name = f"sub-{G.name()}"
    from_graph = G
    relationship_filter = "*"

    # Progress bar
    pbar = tqdm(desc="Calculating pagerank ...", total=len(df0))
    p_res = {}

    for i, leid_id in enumerate(result["leid"].keys()):
        node_filter = f"n.{PROP_NAME['leid']} = {leid_id}"
        # Create the filtered projected graph
        subG, subres = gds.beta.graph.project.subgraph(
            subgraph_name,
            from_graph,
            node_filter,
            relationship_filter,
        )

        # Calculate pagerank
        pr_res = gds.pageRank.write(
            subG,
            writeProperty=PROP_NAME["pagerank"],
            dampingFactor=0.85,
            # maxIterations=44,
            # tolerance=0.0000001,  # default: 0.0000001
            scaler=None,
        )

        # Calculate weighted pagerank
        prw_res = gds.pageRank.write(
            subG,
            writeProperty=PROP_NAME["pagerank_weighted"],
            relationshipWeightProperty="delay_days_q95i",
            dampingFactor=0.85,
            # maxIterations=44,
            # tolerance=0.0000001,
            scaler=None,
        )

        # # Calculate scaled pagerank
        # prs_res = gds.pageRank.write(
        #     subG,
        #     writeProperty=PROP_NAME["pagerank_scaled"],
        #     dampingFactor=0.85,
        #     maxIterations=44,
        #     tolerance=0.0000001,
        #     scaler="MinMax",  # normalizes each score to a value between 0 and 1
        # )

        # Calculate weighted scaled pagerank
        prws_res = gds.pageRank.write(
            subG,
            writeProperty=PROP_NAME["pagerank_weighted_scaled"],
            relationshipWeightProperty="delay_days_q95i",
            dampingFactor=0.85,
            # maxIterations=44,
            # tolerance=0.0000001,
            scaler="MinMax",
        )

        # Update progress bar
        pbar.update(1)
        subG.drop()

        if i == 0:
            p_res["pr"] = pr_res
            p_res["prw"] = prw_res
            # p_res["prs"] = prs_res
            p_res["prws"] = prws_res

    pbar.close()
    display(p_res["pr"])
    print(f"Pagerank have been calculated successfully!")
    result["pagerank"] = p_res

    # # ! Avg Community Pagerank have not any sense. It's the same for all WCC
    # print(f"Calculating avg pagerank for WCC communities ...")
    # query_string = (
    #     f"MATCH (n) "
    #     f"WHERE n.{PROP_NAME['leid']} <> 0 "
    #     f"WITH n.{PROP_NAME['leid']} AS comm, collect(n) AS nodes, "
    #     f"round(avg(n.{PROP_NAME['pagerank_weighted']}), 6) AS avg "
    #     f"FOREACH(n IN nodes | SET n.{PROP_NAME['leid_pagerank_avg']}=avg)"
    # )
    # gds.run_cypher(query_string)

    print(f"Get Leiden communities min - max pagerank ...")
    query_string = (
        f"MATCH (n) "
        f"WHERE n.{PROP_NAME['leid']} <> 0 "
        f"RETURN n.{PROP_NAME['leid']} AS leid_id, count(n) AS size, "
        # f"n.{PROP_NAME['leid_pagerank_avg']}, "
        f"max(n.{PROP_NAME['pagerank_weighted']}) AS max_prw, "
        f"min(n.{PROP_NAME['pagerank_weighted']}) AS min_prw "
        f"ORDER BY size DESC "
        f"LIMIT 250"
    )
    df = gds.run_cypher(query_string)
    display(df.head(14))

    """
    # ! Remove the PAGERANK property
    prop_name = PROP_NAME["leid_pagerank_avg"]
    print(f"Remove calculated property '{prop_name}' ...")
    query_string = (
        f"CALL apoc.periodic.iterate("
        f"'MATCH (n) WHERE n.{prop_name} IS NOT NULL RETURN n',"
        f"'REMOVE n.{prop_name}',"
        f"{{batchSize:50000}})"
    )
    gds.run_cypher(query_string)
    """

    G.drop()
    print("Done.")
    return result


# %% RUN ========================================================================
if __name__ == "__main__":
    result = main()
