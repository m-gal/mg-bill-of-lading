"""
    Apply Graph Data Science library
    for CENTRALITY BETWEENNESS algorithm
    to the Neo4j BoL GraphDB through the GDS python client
    ! Does not make sense for small and VERY BIG communities
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
        # "Carrier": {"properties": ["_wcc_id"]},
        "Shipper": {"properties": ["_wcc_id"]},
        "Consignee": {"properties": ["_wcc_id"]},
        "NotifyParty": {"properties": ["_wcc_id"]},
    }
    RELS_SPEC = {
        # "CARRIES_FOR": {"orientation": "UNDIRECTED"},
        # "SHIPS_BY": {"orientation": "UNDIRECTED"},
        "SHIPS_FOR": {"orientation": "UNDIRECTED"},
        "NOTIFY_FOR": {"orientation": "UNDIRECTED"},
    }
    PROP_NAME = {
        "betweenness": "_between",
        "wcc": "_wcc_id",
        "wcc_size": "_wcc_size",
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
    # * BETWEENNESS ------------------------------------------------------------
    """
    Betweenness centrality is a way of detecting the amount of influence a node
    has over the flow of information in a graph. It is often used to find nodes
    that serve as a bridge from one part of a graph to another.

    The algorithm calculates shortest paths between all pairs of nodes in a graph.
    Each node receives a score, based on the number of shortest paths
    that pass through the node. Nodes that more frequently lie on shortest paths
    between other nodes will have higher betweenness centrality scores.
    """
    print(f"Derive existing WCC IDs and count of components ...")
    query_string = (
        f"MATCH (n) "
        f"WHERE n.{PROP_NAME['wcc']} IS NOT NULL "
        f"RETURN n.{PROP_NAME['wcc']} as wcc_id, count(n) AS count "
        f"ORDER BY count DESC"
    )
    df = gds.run_cypher(query_string)
    print(f"There are {len(df): ,} Weakly Connected Components")
    display(df.head(14))

    df0 = df[df["count"] != 1].head(14)  #!
    result["wcc"] = dict(zip(df0["wcc_id"].values, df0["count"].values))

    # df1 = df[df["count"] == 1]
    # result["wcc_empty"] = df1["wcc_id"].to_list()
    # print(f"Create property '{PROP_NAME['pagerank']}' = 0 for empty WCC ...")
    # query_string = (
    #     f"MATCH (n) "
    #     f"WHERE n.{PROP_NAME['wcc']} IN {result['wcc_empty']} "
    #     f"WITH collect(n) AS nodes "
    #     f"FOREACH(n IN nodes | SET n.{PROP_NAME['pagerank']}=0)"
    # )
    # gds.run_cypher(query_string)

    # Filtered graph spec
    subgraph_name = f"sub-{G.name()}"
    from_graph = G
    relationship_filter = "*"

    # Progress bar
    pbar = tqdm(desc="Calculating pagerank ...", total=len(df0))

    for i, wcc_id in enumerate(result["wcc"].keys()):
        # print(i, wcc_id)
        node_filter = f"n.{PROP_NAME['wcc']} = {wcc_id}"
        # Create the filtered projected graph
        subG, subres = gds.beta.graph.project.subgraph(
            subgraph_name,
            from_graph,
            node_filter,
            relationship_filter,
        )

        # Calculate betweenness
        _res = gds.betweenness.write(
            subG,
            writeProperty=PROP_NAME["betweenness"],
        )

        # Update progress bar
        pbar.update(1)
        subG.drop()

        if i == 0:
            result["betweenness"] = _res

    pbar.close()
    display(result["betweenness"])
    print(f"Betweenness have been calculated successfully!")

    print(f"Get WCC communities min - max pagerank ...")
    query_string = (
        f"MATCH (n) "
        f"WHERE n.{PROP_NAME['wcc']} <> 0 "
        f"RETURN n.{PROP_NAME['wcc']} AS wcc_id, count(n) AS size, "
        f"max(n.{PROP_NAME['betweenness']}) AS max_prw, "
        f"min(n.{PROP_NAME['betweenness']}) AS min_prw "
        f"ORDER BY size DESC "
        f"LIMIT 250"
    )
    df = gds.run_cypher(query_string)
    display(df.head(14))

    """
    # ! Remove the BETWEENNESS property
    prop_name = PROP_NAME["betweenness"]
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
