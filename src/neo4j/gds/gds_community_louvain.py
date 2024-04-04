"""
    Apply Graph Data Science library
    for COMMUNITY DETECTION
    LOUVAIN COMMUNITY algorithms
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


# %% MAIN -----------------------------------------------------------------------
def main():

    GRAPH_NAME = "mg-bol-louvain"
    NODES_SPEC = {
        # "Carrier": {"properties": "name_is_na"},
        "Shipper": {"properties": "name_is_na"},
        "Consignee": {"properties": "name_is_na"},
        "NotifyParty": {"properties": "name_is_na"},
    }
    RELS_SPEC = {
        # "CARRIES_FOR": {"orientation": "UNDIRECTED"},
        # "SHIPS_BY": {"orientation": "UNDIRECTED"},
        "SHIPS_FOR": {"orientation": "UNDIRECTED"},
        "NOTIFY_FOR": {"orientation": "UNDIRECTED"},
    }
    PROP_NAME = {"louv": ["_louv_id", "_louv_size"]}

    prop_name = PROP_NAME["louv"][0]
    prop_size = PROP_NAME["louv"][1]
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
    # * LOUVAIN COMMUNITY ------------------------------------------------------
    """
    The Louvain method is an algorithm to detect communities in large networks.
    It maximizes a modularity score for each community, where the modularity
    quantifies the quality of an assignment of nodes to communities.
    This means evaluating how much more densely connected the nodes within
    a community are, compared to how connected they would be in a random network.

    The Louvain algorithm is a hierarchical clustering algorithm,
    that recursively merges communities into a single node and executes
    the modularity clustering on the condensed graphs.
    """
    print(f"Start: LOUVAIN COMMUNITY as '{prop_name}' ...")
    louv_res = gds.louvain.write(
        subG,
        writeProperty=prop_name,
        maxLevels=2,  # deafault: 10
    )
    result["louv"] = louv_res
    print(f"Done: LOUVAIN COMMUNITY as '{prop_name}' ...")
    display(result["louv"])

    # # Create index on LOUVAIN ID property
    # prop_to_index = dict.fromkeys(NODES_SPEC.keys(), prop_name)
    # for node, prop in prop_to_index.items():
    #     query_string = (
    #         f"CREATE BTREE INDEX btindex{node} IF NOT EXISTS "
    #         f"FOR (n:{node}) "
    #         f"ON (n.{prop})"
    #     )
    #     print(query_string)
    #     gds.run_cypher(query_string)

    print(f"Create a new property with the community size ...")
    query_string = (
        f"MATCH (n) "
        f"WHERE n.{prop_name} IS NOT NULL "
        f"WITH n.{prop_name} AS comm, count(n) AS size, collect(n) AS nodes "
        f"FOREACH(n IN nodes | SET n.{prop_size}=size)"
    )
    gds.run_cypher(query_string)

    print(f"View some instances of LOUVAIN COMMUNITY:")
    query_string = (
        f"MATCH (n) "
        f"WHERE (n.{prop_name}) > 0 "
        f"RETURN labels(n) AS LABEL, "
        f"(CASE WHEN n.code IS NOT NULL THEN n.code ELSE n.name END) AS NAME, "
        f"n.{prop_name} as {prop_name} "
        f"ORDER BY n.{prop_name} DESC "
        f"LIMIT 250"
    )
    df = gds.run_cypher(query_string)
    display(df.head(14))

    print(f"View LOUVAIN COMMUNITY with highest number of nodes:")
    query_string = (
        f"MATCH (n) "
        f"WHERE n.{prop_name} IS NOT NULL "
        f"RETURN n.{prop_name} as louv_id, count(n) AS count "
        f"ORDER BY count DESC"
    )
    df_louv = gds.run_cypher(query_string)
    display(df_louv.head(24))
    result["louv_df"] = df_louv

    print(f"Remove temporally created property 'n.name_is_na' ...")
    query_string = (
        f"CALL apoc.periodic.iterate("
        f"'MATCH (n) RETURN n',"
        f"'REMOVE n.name_is_na',"
        f"{{batchSize:50000}})"
    )
    gds.run_cypher(query_string)

    """
    # ! Remove the LOUVAIN COMMUNITY property
    print(f"Remove calculated property '{prop_name}' ...")
    query_string = (
        f"CALL apoc.periodic.iterate("
        f"'MATCH (n) RETURN n',"
        f"'REMOVE n.{prop_name}, n.{prop_size}',"
        f"{{batchSize:50000}})"
    )
    gds.run_cypher(query_string)
    """

    # Clean up graph catalog ---------------------------------------------------
    G.drop()
    subG.drop()
    print("Done.")
    return result


# %% RUN ========================================================================
if __name__ == "__main__":
    result = main()
