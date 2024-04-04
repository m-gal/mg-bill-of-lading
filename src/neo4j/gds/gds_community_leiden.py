"""
    Apply Graph Data Science library
    for COMMUNITY DETECTION
    LEIDEN COMMUNITY algorithms
    to the Neo4j BoL GraphDB through the GDS python client
    @author: mikhail.galkin 2022
"""

# %% Setup ----------------------------------------------------------------------
from IPython.display import display
from graphdatascience import GraphDataScience

# %% Load project's stuff -------------------------------------------------------
from mgbol.neo4j.neo_driver import neo4j_uri
from mgbol.neo4j.neo_driver import neo4j_user
from mgbol.neo4j.neo_driver import neo4j_pass


# %% Custom functiom ------------------------------------------------------------
def stream_result(leid_res):
    import pandas as pd

    nunique = leid_res["communityId"].nunique()
    df = pd.DataFrame(leid_res["communityId"].value_counts()).reset_index()
    df.rename(columns={"index": "id", "communityId": "size"}, inplace=True)
    bins = [
        0,
        1,
        2,
        3,
        10,
        100,
        250,
        500,
        1000,
        2000,
        5000,
        10000,
        50000,
        100000,
        500000,
        1000000,
    ]
    df["size_bined"] = pd.cut(df["size"], bins)
    g = df.groupby(["size_bined"])["size"].agg(["count", "sum", "mean"])
    g.reset_index(inplace=True)
    columns = {"count": "n_comm", "sum": "n_nodes", "mean": "mean_nodes"}
    g.rename(columns=columns, inplace=True)
    g["prc_comm"] = g["n_comm"] / sum(g["n_comm"]) * 100
    g["prc_nodes"] = g["n_nodes"] / sum(g["n_nodes"]) * 100
    print(f"# unique communities: {nunique:,}")
    print(f"Top 25:")
    display(df[:24])
    display(g)
    return g


# %% MAIN -----------------------------------------------------------------------
def main():

    GRAPH_NAME = "mg-bol-leiden"
    NODES_SPEC = {
        "Shipper": {"properties": ["name_is_na", "_wcc_id"]},
        "Consignee": {"properties": ["name_is_na", "_wcc_id"]},
        "NotifyParty": {"properties": ["name_is_na", "_wcc_id"]},
    }
    RELS_SPEC = {
        "SHIPS_FOR": {
            "orientation": "UNDIRECTED",
            # "properties": {"delay_days_q95i": {"property": "delay_days_q95i"}},
        },
        "NOTIFY_FOR": {
            "orientation": "UNDIRECTED",
            # "properties": {"delay_days_q95i": {"property": "delay_days_q95i"}},
        },
    }
    PROP_NAME = {"leid": ["_leid_id", "_leid_size"]}

    prop_name = PROP_NAME["leid"][0]
    prop_size = PROP_NAME["leid"][1]
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

    # For filtering the projected grpah from nodes with 'N/A' names
    # because such Nodes will be have a big degree & pagerank w/o sense
    # Due the filter of projected graph can not able manipulate with Strings
    # we create a temporal property for Nodes which name='N/A'
    print(f"Create temporal nodes property 'name_is_na' ...")
    query_string = (
        f"MATCH (n) "
        f"WITH count(n) AS size, collect(n) AS nodes "
        f"FOREACH(n IN nodes | SET n.name_is_na = (CASE "
        f"WHEN n.name = 'N/A' THEN 1 "
        f"ELSE 0 END))"
    )
    gds.run_cypher(query_string)

    # Project the graph into the GDS Graph Catalog
    print(f"Create the new projected graph '{GRAPH_NAME}'...")
    # Where G is a Graph object, and res is a pd.Series containing metadata
    G, res = gds.graph.project(
        graph_name=GRAPH_NAME,
        node_spec=NODES_SPEC,
        relationship_spec=RELS_SPEC,
    )
    result[GRAPH_NAME] = res
    display(result[GRAPH_NAME])

    # print(f"Derive biggest existing WCC ID ...")
    # query_string = (
    #     f"MATCH (n) "
    #     f"WHERE n._wcc_id IS NOT NULL "
    #     f"RETURN n._wcc_id as wcc_id, count(n) AS count "
    #     f"ORDER BY count DESC "
    #     f"LIMIT 1"
    # )
    # df = gds.run_cypher(query_string)
    # print(f"There are {len(df): ,} Weakly Connected Components")
    # display(df)
    # node_filter = f"n._wcc_id = {df['wcc_id'].iloc[0]}"

    # Filter the projected graph
    subgraph_name = f"sub-{G.name()}"
    from_graph = G
    node_filter = "n.name_is_na = 0"
    relationship_filter = "*"

    print(f"Create the filtered projected graph '{subgraph_name}' ...")
    subG, subres = gds.beta.graph.project.subgraph(
        subgraph_name,
        from_graph,
        node_filter,
        relationship_filter,
    )
    result[subgraph_name] = subres
    display(result[subgraph_name])

    # C O M M U N I T Y  D E T E C T I O N :------------------------------------
    # * LEIDEN COMMUNITY ------------------------------------------------------
    """
    The Leiden algorithm is an algorithm for detecting communities in large networks.
    The algorithm separates nodes into disjoint communities so as to maximize
    a modularity score for each community. Modularity quantifies the quality of
    an assignment of nodes to communities, that is how densely connected nodes
    in a community are, compared to how connected they would be in a random network.

    The Leiden algorithm is a hierarchical clustering algorithm,
    that recursively merges communities into single nodes by greedily optimizing
    the modularity and the process repeats in the condensed graph.
    It modifies the Louvain algorithm to address some of its shortcomings,
    namely the case where some of the communities found by Louvain are not well-connected.
    This is achieved by periodically randomly breaking down communities
    into smaller well-connected ones.
    """
    print(f"Start: LEIDEN COMMUNITY as '{prop_name}' ...")
    leid_res = gds.alpha.leiden.write(
        subG,
        writeProperty=prop_name,
        # best parameteres are: 10 / 50000.0
        maxLevels=10,  # Integer, deafault: 10
        gamma=5000.0,  # Float, default: 1.0
        # relationshipWeightProperty="delay_days_q95i",
    )
    result["leid"] = leid_res
    print(f"Done: LEIDEN COMMUNITY as '{prop_name}' ...")
    # _ = stream_result(leid_res) # when use stream instead write

    # Create index on LEIDEN ID property
    prop_to_index = dict.fromkeys(NODES_SPEC.keys(), prop_name)
    for node, prop in prop_to_index.items():
        query_string = (
            f"CREATE BTREE INDEX btindex{node} IF NOT EXISTS " f"FOR (n:{node}) " f"ON (n.{prop})"
        )
        print(query_string)
        gds.run_cypher(query_string)

    print(f"Create a new property with the community size ...")
    query_string = (
        f"MATCH (n) "
        f"WHERE n.{prop_name} IS NOT NULL "
        f"WITH n.{prop_name} AS comm, count(n) AS size, collect(n) AS nodes "
        f"FOREACH(n IN nodes | SET n.{prop_size}=size)"
    )
    gds.run_cypher(query_string)

    # print(f"View some instances of LEIDEN COMMUNITY:")
    # query_string = (
    #     f"MATCH (n) "
    #     f"WHERE (n.{prop_name}) > 0 "
    #     f"RETURN labels(n) AS LABEL, "
    #     f"(CASE WHEN n.code IS NOT NULL THEN n.code ELSE n.name END) AS NAME, "
    #     f"n.{prop_name} as {prop_name} "
    #     f"ORDER BY n.{prop_name} DESC "
    #     f"LIMIT 250"
    # )
    # df = gds.run_cypher(query_string)
    # display(df.head(14))

    # print(f"View LEIDEN COMMUNITY with highest number of nodes:")
    # query_string = (
    #     f"MATCH (n) "
    #     f"WHERE n.{prop_name} IS NOT NULL "
    #     f"RETURN n.{prop_name} as leid_id, count(n) AS size "
    #     f"ORDER BY size DESC"
    # )
    # df = gds.run_cypher(query_string)
    # display(df.head(25))
    # result["df"] = df

    print(f"View result of LEIDEN COMMUNITY algorithm:")
    query_string = (
        f"MATCH (n) "
        f"WHERE n.{prop_name} IS NOT NULL "
        f"RETURN id(n) AS nodeId, n.{prop_name} as communityId "
    )
    df = gds.run_cypher(query_string)
    g = stream_result(df)
    result["df"] = g

    print(f"Remove temporally created property 'n.name_is_na' ...")
    query_string = (
        f"CALL apoc.periodic.iterate("
        f"'MATCH (n) RETURN n',"
        f"'REMOVE n.name_is_na',"
        f"{{batchSize:50000}})"
    )
    gds.run_cypher(query_string)

    """
    # ! Remove the LEIDEN COMMUNITY property
    prop_name = "_leid_id"
    prop_size = "_leid_size"
    print(f"Remove calculated property {prop_name}...")
    query_string = (
        f"CALL apoc.periodic.iterate("
        f"'MATCH (n) RETURN n',"
        f"'REMOVE n.{prop_name}, n.{prop_size}',"
        f"{{batchSize:50000}})"
    )
    gds.run_cypher(query_string)
    """

    # DROP INDEX ON:Consignee(_leid_id)
    # Clean up graph catalog ---------------------------------------------------
    G.drop()
    subG.drop()
    print("Done.")
    return result


# %% RUN ========================================================================
if __name__ == "__main__":
    result = main()
