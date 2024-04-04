"""
    Apply Graph Data Science library
    for COMMUNITY DETECTION
    LABEL PROPAGATION algorithms
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

    GRAPH_NAME = "mg-bol-lpa"
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
    PROP_NAME = {"lpa": ["_lpa_id", "_lpa_size"]}

    prop_name = PROP_NAME["lpa"][0]
    prop_size = PROP_NAME["lpa"][1]
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
    # * LABEL PROPAGATION COMMUNITY --------------------------------------------
    """
    The Label Propagation algorithm (LPA) is a fast algorithm for finding
    communities in a graph. It detects these communities using network structure
    alone as its guide, and doesn’t require a pre-defined objective function
    or prior information about the communities.

    LPA works by propagating labels throughout the network and forming
    communities based on this process of label propagation.

    The intuition behind the algorithm is that a single label can quickly become
    dominant in a densely connected group of nodes, but will have trouble
    crossing a sparsely connected region. Labels will get trapped inside
    a densely connected group of nodes, and those nodes that end up with
    the same label when the algorithms finish can be considered part
    of the same community.
    """

    print(f"Start: LABEL PROPAGATION COMMUNITY as '{prop_name}' ...")
    lpa_res = gds.labelPropagation.write(
        subG,
        writeProperty=prop_name,
    )
    result["lpa"] = lpa_res
    print(f"Done: LABEL PROPAGATION COMMUNITY as '{prop_name}' ...")
    display(result["lpa"])

    print(f"Create a new property with the community size ...")
    query_string = (
        f"MATCH (n) "
        f"WHERE n.{prop_name} IS NOT NULL "
        f"WITH n.{prop_name} AS comm, count(n) AS size, collect(n) AS nodes "
        f"FOREACH(n IN nodes | SET n.{prop_size}=size)"
    )
    gds.run_cypher(query_string)

    print(f"View some instances of LABEL PROPAGATION COMMUNITY:")
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

    print(f"View LABEL PROPAGATION COMMUNITY with highest number of nodes:")
    query_string = (
        f"MATCH (n) "
        f"WHERE n.{prop_name} IS NOT NULL "
        f"RETURN n.{prop_name} as lpa_id, count(n) AS count "
        f"ORDER BY count DESC"
    )
    df_lpa = gds.run_cypher(query_string)
    display(df_lpa.head(24))
    result["lpa_df"] = df_lpa

    print(f"Remove temporally created property 'n.name_is_na' ...")
    query_string = (
        f"CALL apoc.periodic.iterate("
        f"'MATCH (n) RETURN n',"
        f"'REMOVE n.name_is_na',"
        f"{{batchSize:50000}})"
    )
    gds.run_cypher(query_string)

    """
    # ! Remove the LABEL PROPAGATION COMMUNITY property
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
