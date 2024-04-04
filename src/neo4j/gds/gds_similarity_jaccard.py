"""
    Apply Graph Data Science library
    for SIMILARITY JACCARD algorithms
    to the Neo4j BoL GraphDB
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


# %% MAIN -----------------------------------------------------------------------
def main():

    GRAPH_NAME = "mg-bol-jaccard"
    NODE_PROJECTION = {
        "Carrier": {
            "properties": {
                "delay_days_q95": {"property": "delay_days_q95"},
                "delay_count_ratio": {"property": "delay_count_ratio"},
                "shipment_count": {"property": "shipment_count"},
            }
        },
        "Consignee": {
            "properties": {
                "delay_days_q95": {"property": "delay_days_q95"},
                "delay_count_ratio": {"property": "delay_count_ratio"},
                "shipment_count": {"property": "shipment_count"},
            },
        },
        "NotifyParty": {
            "properties": {
                "delay_days_q95": {"property": "delay_days_q95"},
                "delay_count_ratio": {"property": "delay_count_ratio"},
                "shipment_count": {"property": "shipment_count"},
            },
        },
        "Shipper": {
            "properties": {
                "delay_days_q95": {"property": "delay_days_q95"},
                "delay_count_ratio": {"property": "delay_count_ratio"},
                "shipment_count": {"property": "shipment_count"},
            },
        },
        "PortOfLading": {
            "properties": {
                "delay_days_q95": {"property": "delay_days_q95"},
                "delay_count_ratio": {"property": "delay_count_ratio"},
                "shipment_count": {"property": "shipment_count"},
            },
        },
        "PortOfUnlading": {
            "properties": {
                "delay_days_q95": {"property": "delay_days_q95"},
                "delay_count_ratio": {"property": "delay_count_ratio"},
                "shipment_count": {"property": "shipment_count"},
            },
        },
    }
    RELATIONSHIP_PROJECTION = {
        "SHIPS_FOR": {
            "orientation": "UNDIRECTED",
            "properties": {"delay_days_q95": {"property": "delay_days_q95"}},
        },
        "SHIPS_BY": {
            "orientation": "UNDIRECTED",
            "properties": {"delay_days_q95": {"property": "delay_days_q95"}},
        },
        "CARRIES_FOR": {
            "orientation": "UNDIRECTED",
            "properties": {"delay_days_q95": {"property": "delay_days_q95"}},
        },
        "UNLADING_IN": {
            "orientation": "UNDIRECTED",
            "properties": {"delay_days_q95i": {"property": "delay_days_q95"}},
        },
        "LADING_IN": {
            "orientation": "UNDIRECTED",
            "properties": {"delay_days_q95i": {"property": "delay_days_q95"}},
        },
        "NOTIFY_FOR": {
            "orientation": "UNDIRECTED",
            "properties": {"delay_days_q95i": {"property": "delay_days_q95"}},
        },
    }

    KNN_REL_NAME = "SIMILAR"
    KNN_PROP_NAME = "similar_knn"
    NODE_SIMILARITY_PROP_NAME = "sim_knn_score"

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

    # S I M I L A R I T Y :-----------------------------------------------------
    # * NODE SIMILARITY --------------------------------------------------------
    """
    The Node Similarity algorithm compares a set of nodes based on the nodes
    they are connected to.
    * Two nodes are considered similar if they share many of the same neighbors.
    Node Similarity computes pair-wise similarities based on either
    the Jaccard metric, also known as the Jaccard Similarity Score, or
    the Overlap coefficient, also known as the Szymkiewicz–Simpson coefficient.
    & 2 960 487 nodes: 4.5 hour
    """
    K = 14
    node = ["Shipper", "Consignee"]
    rel_type = f"ALIKE_{node[0].upper()}_{node[1].upper()}"

    res = gds.nodeSimilarity.write(
        G,
        topK=K,  # of neighbors per nodes
        similarityMetric="JACCARD",
        nodeLabels=node,
        writeProperty=NODE_SIMILARITY_PROP_NAME,
        writeRelationshipType=rel_type,
        similarityCutoff=0.01,
    )
    display(res)
    query_string = (
        f"MATCH (n1:{node})-[r:{rel_type}]->(n2:{node}) "
        f"WHERE r.{NODE_SIMILARITY_PROP_NAME} IS NOT NULL "
        f"RETURN labels(n1), n1.name, n2.name, "
        f"r.{NODE_SIMILARITY_PROP_NAME} "
        f"ORDER BY r.{NODE_SIMILARITY_PROP_NAME} ASC "
        f"LIMIT (1000)"
    )
    df = gds.run_cypher(query_string)
    display(df.sample(14))

    # Clean up graph catalog
    G.drop()
    print("Done.")


# %% RUN ========================================================================
if __name__ == "__main__":
    main()
