"""
    Apply Graph Data Science library
    for CENTRALITY PAGERANK algorithm for PORTS
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
def get_pagerank_port(
    nodes_spec,
    rels_spec,
):

    GRAPH_NAME = "mg-bol-pagerank-ports"
    PROP_NAME = {
        "pagerank": "_pagerankX",
        "pagerank_scaled": "_pr_scaledX",
        "pagerank_weighted": "_pr_weightedX",
        "pagerank_weighted_scaled": "_pr_wescaledX",
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
        nodes_spec,
        rels_spec,
    )
    result[GRAPH_NAME] = res
    display(result[GRAPH_NAME])

    # # Check the graph structure
    # print(f"Chek: WEAKLY CONNECTED COMPONENTS ...")
    # wcc_res = gds.wcc.stream(G)
    # result["wcc"] = wcc_res
    # n_wcc = result["wcc"]["componentId"].value_counts()
    # display(n_wcc)
    # if len(n_wcc) == 1:
    #     print(f"It's Ok! We can implement the PageRank algorithm")
    # else:
    #     return n_wcc

    # C E N T R A L I T Y :-----------------------------------------------------
    # * PAGERANK ---------------------------------------------------------------
    """
    ! PageRank can lead to unexpected result on graphs with multiple components.
    """
    p_res = {}

    # Calculate pagerank
    pr_res = gds.pageRank.write(
        G,
        writeProperty=PROP_NAME["pagerank"],
        dampingFactor=0.85,
        # maxIterations=44,
        # tolerance=0.0000001,  # default: 0.0000001
        scaler=None,
    )

    # Calculate weighted pagerank
    prw_res = gds.pageRank.write(
        G,
        writeProperty=PROP_NAME["pagerank_weighted"],
        relationshipWeightProperty="delay_days_q95i",
        dampingFactor=0.85,
        # maxIterations=44,
        # tolerance=0.0000001,
        scaler=None,
    )

    # Calculate weighted scaled pagerank
    prws_res = gds.pageRank.write(
        G,
        writeProperty=PROP_NAME["pagerank_weighted_scaled"],
        relationshipWeightProperty="delay_days_q95i",
        dampingFactor=0.85,
        # maxIterations=44,
        # tolerance=0.0000001,
        scaler="MinMax",
    )

    p_res["pr"] = pr_res
    p_res["prw"] = prw_res
    p_res["prws"] = prws_res

    display(p_res["pr"])
    print(f"Pagerank have been calculated successfully!")
    result["pagerank"] = p_res

    print(f"Get some Ports Of Lading w/ PageRank ...")
    query_string = (
        f"MATCH (n: PortOfLading) "
        f"WHERE n.{PROP_NAME['pagerank']} IS NOT NULL "
        f"RETURN n.name AS PortOfLading, "
        f"n.code AS code, "
        f"n.{PROP_NAME['pagerank']} AS pagerank, "
        f"n.{PROP_NAME['pagerank_weighted']} AS pr_weighted, "
        f"n.{PROP_NAME['pagerank_weighted_scaled']} AS pr_weighted_scaled "
        f"ORDER BY pagerank DESC "
        f"LIMIT 250"
    )
    df = gds.run_cypher(query_string)
    display(df.head(14))

    print(f"Get some Ports Of Unlading w/ PageRank ...")
    query_string = (
        f"MATCH (n: PortOfUnlading) "
        f"WHERE n.{PROP_NAME['pagerank']} IS NOT NULL "
        f"RETURN n.name AS PortOfUnlading, "
        f"n.code AS code, "
        f"n.{PROP_NAME['pagerank']} AS pagerank, "
        f"n.{PROP_NAME['pagerank_weighted']} AS pr_weighted, "
        f"n.{PROP_NAME['pagerank_weighted_scaled']} AS pr_weighted_scaled "
        f"ORDER BY pagerank DESC "
        f"LIMIT 250"
    )
    df = gds.run_cypher(query_string)
    display(df.head(14))

    # Remove the PAGERANK-X property from non Carriers nodes
    for e in ["Shipper", "Carrier"]:
        for prop_name in PROP_NAME.values():
            print(f"Remove property '{prop_name}' from {e} ...")
            query_string = (
                f"CALL apoc.periodic.iterate("
                f"'MATCH (n:{e}) WHERE n.{prop_name} IS NOT NULL RETURN n',"
                f"'REMOVE n.{prop_name}',"
                f"{{batchSize:50000}})"
            )
            gds.run_cypher(query_string)

    # Rename the PAGERANK-X property
    for e in ["PortOfLading", "PortOfUnlading"]:
        for prop_name in PROP_NAME.values():
            prop_name_new = prop_name[:-1]
            print(f"{e}: rename '{prop_name}' to '{prop_name_new}' ...")
            query_string = (
                f"MATCH (n:{e}) "
                f"WHERE n.{prop_name} IS NOT NULL "
                f"WITH collect(n) AS nodes "
                f"CALL apoc.refactor.rename.nodeProperty("
                f"'{prop_name}', "
                f"'{prop_name_new}', "
                f"nodes, "
                f"{{batchSize:50000}}) "
                f"YIELD committedOperations "
                f"RETURN committedOperations"
            )
            gds.run_cypher(query_string)

    """
    # ! Remove the PAGERANK property
    for e in ["PortOfLading", "PortOfUnlading"]:
        for prop_name in PROP_NAME.values():
            print(f"Remove property '{prop_name}' from {e} ...")
            query_string = (
                f"CALL apoc.periodic.iterate("
                f"'MATCH (n:{e}) WHERE n.{prop_name} IS NOT NULL RETURN n',"
                f"'REMOVE n.{prop_name}',"
                f"{{batchSize:50000}})"
            )
            gds.run_cypher(query_string)
    """

    G.drop()
    print("Done.")
    return result


def main():
    print(f"\nCalculate pagerank for Ports Of Lading -------------------------")
    result_pol = get_pagerank_port(
        nodes_spec={
            "Carrier": {},
            "Shipper": {},
            "PortOfLading": {},
        },
        rels_spec={
            "CARRIES_FROM": {
                "orientation": "NATURAL",
                "properties": {"delay_days_q95i": {"property": "delay_days_q95i"}},
            },
            "SHIPS_FROM": {
                "orientation": "NATURAL",
                "properties": {"delay_days_q95i": {"property": "delay_days_q95i"}},
            },
        },
    )

    print(f"\nCalculate pagerank for Ports Of Unlading -----------------------")
    result_poul = get_pagerank_port(
        nodes_spec={
            "Carrier": {},
            "Shipper": {},
            "PortOfUnlading": {},
        },
        rels_spec={
            "CARRIES_TO": {
                "orientation": "NATURAL",
                "properties": {"delay_days_q95i": {"property": "delay_days_q95i"}},
            },
            "SHIPS_TO": {
                "orientation": "NATURAL",
                "properties": {"delay_days_q95i": {"property": "delay_days_q95i"}},
            },
        },
    )

    return (result_pol, result_poul)


# %% RUN ========================================================================
if __name__ == "__main__":
    results = main()
