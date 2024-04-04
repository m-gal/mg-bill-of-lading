"""
    Apply Graph Data Science library
    for calculating some properties
    for the Neo4j BoL GraphDB
    through the GDS python client
    @author: mikhail.galkin 2022
"""

# %% Setup ----------------------------------------------------------------------
from IPython.display import display
from graphdatascience import GraphDataScience

# %% Load project's stuff -------------------------------------------------------
from mgbol.neo4j.neo_driver import neo4j_uri
from mgbol.neo4j.neo_driver import neo4j_user
from mgbol.neo4j.neo_driver import neo4j_pass

# %% RUN ========================================================================
if __name__ == "__main__":

    GRAPH_NAME = "mg-bol"
    NODE_PROJECTION = "*"
    RELATIONSHIP_PROJECTION = "*"

    # Follow the convention to name `GraphDataScience` object as `gds`
    gds = GraphDataScience(neo4j_uri, auth=(neo4j_user, neo4j_pass))
    print(f"GDS version: {gds.version()}")

    # Drop all graphs in memory
    print(f"Check graphs in memory ...")
    graphs = gds.graph.list()["graphName"].to_list()
    if len(graphs) > 0:
        for graph in graphs:
            print(f"Dropped '{graph}' graph ...")
            G = gds.graph.get(graph)
            G.drop()
    else:
        print(f"No one projected graph in memory ...")

    # Project the graph into the GDS Graph Catalog
    print(f"Create the new projected graph '{GRAPH_NAME}'...")
    # Where G is a Graph object, and res is a pd.Series containing metadata
    G, res = gds.graph.project(
        graph_name=GRAPH_NAME,
        node_spec=NODE_PROJECTION,
        relationship_spec=RELATIONSHIP_PROJECTION,
    )
    display(res)

# %% Get Ports scores -----------------------------------------------------------
print(f"Calculate Ports' scores ...")
query_string = (
    f"MATCH (o:PortOfLading) "
    f"RETURN labels(o) AS node, o.code as port_code, o.name as name, "
    f"o._pr_weighted as pagerank, "
    f"o._degree as connection_count, "
    f"o.container_count as container_count, "
    f"o.teu_sum as teu_sum, "
    f"o.delay_days_q50 as delay_days_q50, o.delay_days_q95 as delay_days_q95 "
    f"ORDER BY pagerank "
    # f"DESC LIMIT 10 "
    f"UNION ALL "
    f"MATCH (d:PortOfUnlading) "
    f"RETURN labels(d) AS node, d.code as port_code, d.name as name, "
    f"d._pr_weighted as pagerank, "
    f"d._degree as connection_count, "
    f"d.container_count as container_count, "
    f"d.teu_sum as teu_sum, "
    f"d.delay_days_q50 as delay_days_q50, d.delay_days_q95 as delay_days_q95 "
    f"ORDER BY pagerank "
    # f"DESC LIMIT 10"
)
df = gds.run_cypher(query_string)
display(df)

# %% Get OD-pairs of ports scores
print(f"Calculate Ports' scores ...")
query_string = (
    f"MATCH p = (o:PortOfLading)--[:Carrier]--(d:PortOfUnlading) "
    # f"RETURN o.code as port_code_o, o.name as name_o, "
    # f"o._pr_weighted as pagerank_o, "
    # f"o._degree as connection_count_o, "
    # f"o.container_count as container_count_o, "
    # f"o.delay_days_q95 as delay_days_q95_o, "
    # f"d.code as port_code_d, d.name as name_d, "
    # f"d._pr_weighted as pagerank_d, "
    # f"d._degree as connection_count_d, "
    # f"d.container_count as container_count_d, "
    # f"d.delay_days_q95 as delay_days_q95_d, "
    # f"((o._pr_weighted + d._pr_weighted)/2) as pagerank_od "
    # f"ORDER BY pagerank_od "
    # f"DESC LIMIT 10"
    f"RETURN p"
)
df = gds.run_cypher(query_string)
display(df)
