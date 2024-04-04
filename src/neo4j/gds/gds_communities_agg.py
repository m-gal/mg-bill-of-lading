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

# %% Embedding of community score for each community member
print(f"Find avg for the weighted, scaled pagerank for Louvain community ...")
query_string = (
    f"MATCH (n) "
    f"WHERE n._louvainId IS NOT NULL "
    f"WITH n._louvainId AS comm, count(n) AS size, "
    f"round(avg(n._pr_wscaled)*1000000, 4) AS avg, collect(n) AS nodes "
    f"FOREACH(n IN nodes | SET n._louv_comm_size=size) "
    f"FOREACH(n IN nodes | SET n._louv_pr_score=avg)"
)
gds.run_cypher(query_string)

print(f"Get communities avg pagerank ...")
query_string = (
    f"MATCH (n) "
    f"WHERE n._louvainId IS NOT NULL "
    f"RETURN n._louvainId AS community_id, count(n) AS community_size, "
    f"n._louv_comm_size, "
    f"n._louv_pr_score, "
    f"max(n._pr_wscaled) AS max_comm_pgr, "
    f"min(n._pr_wscaled) AS min_comm_pgr "
    f"ORDER BY n._louv_comm_size DESC "
    f"LIMIT 25"
)
df = gds.run_cypher(query_string)
display(df)

# %% Assing the financial score for Consignee and get the community avg score
print(f"Generate & assing the financial score for Consignees ...")
# # Generate random score
# query_string = (
#     f"MATCH (n: Consignee) "
#     f"WITH collect(n) AS nodes, "
#     f"toInteger(ceil(rand() * (999 - 100)) + 100) AS score "
#     f"FOREACH(n IN nodes | SET n._finhealth_score = score)"
# )
# gds.run_cypher(query_string)

# Generate random but grouped score
query_string = (
    f"MATCH (n: Consignee) "
    f"WITH n._louvainId AS comm, count(n) AS size, collect(n) AS nodes, "
    f"toInteger(rand() * (999 - 1) + 1) AS score, "
    f"toInteger(rand() * (100 - 1) + 1) AS score0, "
    f"toInteger(rand() * (200 - 100) + 100) AS score1, "
    f"toInteger(rand() * (300 - 200) + 200) AS score2, "
    f"toInteger(rand() * (400 - 300) + 300) AS score3, "
    f"toInteger(rand() * (500 - 400) + 400) AS score4, "
    f"toInteger(rand() * (600 - 500) + 500) AS score5, "
    f"toInteger(rand() * (700 - 600) + 600) AS score6, "
    f"toInteger(rand() * (800 - 700) + 700) AS score7, "
    f"toInteger(rand() * (900 - 800) + 800) AS score8, "
    f"toInteger(rand() * (999 - 900) + 900) AS score9 "
    f"FOREACH(n IN nodes | SET n._finhealth_score = (CASE "
    f"WHEN n._louvainId < 100000 THEN score0 "
    f"WHEN 100000 < n._louvainId < 500000 THEN score1 "
    f"WHEN 500000 < n._louvainId < 1000000 THEN score2 "
    f"WHEN 1000000 < n._louvainId < 2000000 THEN score3 "
    f"WHEN 2000000 < n._louvainId < 2150000 THEN score4 "
    f"WHEN 2150000 < n._louvainId < 2350000 THEN score5 "
    f"WHEN 2350000 < n._louvainId < 2450000 THEN score6 "
    f"WHEN 2450000 < n._louvainId < 2600000 THEN score7 "
    f"WHEN 2600000 < n._louvainId < 2750000 THEN score8 "
    f"WHEN 2750000 < n._louvainId < 2850000 THEN score9 "
    f"ELSE score END))"
)
gds.run_cypher(query_string)

# Make score more like bimodal
query_string = (
    f"MATCH (n: Consignee) "
    f"WITH n, "
    f"CASE n.name STARTS WITH 'S' "
    f"WHEN n._finhealth_score < 100 THEN 200 "
    f"WHEN 100 < n._finhealth_score <= 200 THEN 100 "
    f"WHEN 200 < n._finhealth_score <= 300 THEN 0 "
    f"WHEN 300 < n._finhealth_score <= 400 THEN -100 "
    f"WHEN 400 < n._finhealth_score <= 500 THEN 200 "
    f"WHEN 500 < n._finhealth_score <= 600 THEN 100 "
    f"WHEN 600 < n._finhealth_score <= 700 THEN 0 "
    f"WHEN 700 < n._finhealth_score <= 800 THEN -100 "
    f"WHEN 800 < n._finhealth_score <= 900 THEN -200 "
    f"WHEN 900 < n._finhealth_score <= 1000 THEN -250 "
    f"END AS delta "
    f"SET n._finhealth_score = (n._finhealth_score + delta)"
)
gds.run_cypher(query_string)

print(f"Calculate avg for the finhealth score for Louvain community ...")
query_string = (
    f"MATCH (n: Consignee) "
    f"WITH n._louvainId AS comm, count(n) AS size, collect(n) AS nodes, "
    f"toInteger(avg(n._finhealth_score)) AS avg "
    f"FOREACH(n IN nodes | SET n._louv_fin_score=avg)"
)
gds.run_cypher(query_string)

print(f"Get communities avg financial health score ...")
query_string = (
    f"MATCH (n: Consignee) "
    f"RETURN n._louvainId AS community_id, count(n) AS consignee_count, "
    f"n._louv_comm_size, "
    f"n._louv_pr_score, "
    f"n._louv_fin_score, "
    f"max(n._finhealth_score) AS max_finhealth_score, "
    f"min(n._finhealth_score) AS min_finhealth_score "
    f"ORDER BY n._louv_comm_size DESC "
    f"LIMIT 50"
)
df = gds.run_cypher(query_string)
display(df)
print("Done.")

# %% View communities
comms = [
    "_communityIds",
    "_louvainId",
    "_lpaId",
    "_modularityId",
    "_wcc_id",
]

for comm in comms:
    print(f"Get < {comm} > communities ...")
    query_string = (
        f"MATCH (n) "
        f"RETURN n.{comm} AS community_id, "
        f"count(n) AS community_size "
        f"ORDER BY community_size DESC "
        # f"LIMIT 50"
    )
    df = gds.run_cypher(query_string)
    display(df)
