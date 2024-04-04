"""
    Apply Graph Data Science library
    for CENTRALITY DEGREE algorithm
    to the Neo4j BoL GraphDB through the GDS python client
    @author: mikhail.galkin
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

    GRAPH_NAME = "mg-bol-degree"
    NODES_SPEC = {
        "Carrier": {},
        "Consignee": {},
        "Shipper": {},
        "NotifyParty": {},
        "PortOfLading": {},
        "PortOfUnlading": {},
    }
    RELS_SPEC = "*"
    PROP_NAME = {"degree": "_degree"}

    prop_name = PROP_NAME["degree"]
    result = {}

    # Follow the convention to name `GraphDataScience` object as `gds`
    gds = GraphDataScience(neo4j_uri, auth=(neo4j_user, neo4j_pass))
    print(f"GDS version: {gds.version()}")

    # Check & drop all graphs in memory
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

    # Calculate Centrality Degree
    print(f"Start: DEGREE as '{prop_name}' ...")
    res_degree = gds.degree.write(
        G,
        writeProperty=prop_name,
        orientation="UNDIRECTED",
    )
    result["degree"] = res_degree
    print(f"Done: DEGREE as '{prop_name}' ...")
    display(res_degree)

    print(f"View nodes count with the max Centrality Degree:")
    query_string = (
        f"MATCH (n) "
        f"RETURN n.{prop_name} as DEGREE, LABELS(n) as LABEL, n.name as NAME "
        f"ORDER BY n.{prop_name} DESC "
        f"LIMIT 14"
    )
    df = gds.run_cypher(query_string)
    display(df)

    G.drop()
    print("Done.")
    return result


# %% RUN ========================================================================
if __name__ == "__main__":
    result = main()
