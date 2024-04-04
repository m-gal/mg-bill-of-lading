"""
    Apply Graph Data Science library
    for SIMILARITY K-NN algorithms
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

    GRAPH_NAME = "mg-bol-knn"
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
                "teu_sum": {"property": "teu_sum"},
            },
        },
        "PortOfUnlading": {
            "properties": {
                "delay_days_q95": {"property": "delay_days_q95"},
                "delay_count_ratio": {"property": "delay_count_ratio"},
                "shipment_count": {"property": "shipment_count"},
                "teu_sum": {"property": "teu_sum"},
            },
        },
    }
    RELATIONSHIP_PROJECTION = {
        "SHIPS_FOR": {  #
            "orientation": "NATURAL",
            "properties": {"delay_days_q95": {"property": "delay_days_q95"}},
        },
        "SHIPS_BY": {  #
            "orientation": "NATURAL",
            "properties": {"delay_days_q95": {"property": "delay_days_q95"}},
        },
        "SHIPS_FROM": {  #
            "orientation": "NATURAL",
            "properties": {"delay_days_q95": {"property": "delay_days_q95"}},
        },
        "SHIPS_TO": {  #
            "orientation": "NATURAL",
            "properties": {"delay_days_q95": {"property": "delay_days_q95"}},
        },
        "CARRIES_FOR": {  #
            "orientation": "NATURAL",
            "properties": {"delay_days_q95": {"property": "delay_days_q95"}},
        },
        "CARRIES_FROM": {  #
            "orientation": "NATURAL",
            "properties": {"delay_days_q95": {"property": "delay_days_q95"}},
        },
        "CARRIES_TO": {  #
            "orientation": "NATURAL",
            "properties": {"delay_days_q95": {"property": "delay_days_q95"}},
        },
        "NOTIFY_FOR": {  #
            "orientation": "NATURAL",
            "properties": {"delay_days_q95i": {"property": "delay_days_q95"}},
        },
        "NOTIFY_IN": {  #
            "orientation": "NATURAL",
            "properties": {"delay_days_q95i": {"property": "delay_days_q95"}},
        },
        "UNLADING_FOR": {  #
            "orientation": "NATURAL",
            "properties": {"delay_days_q95i": {"property": "delay_days_q95"}},
        },
        "LADING_FOR": {  #
            "orientation": "NATURAL",
            "properties": {"delay_days_q95i": {"property": "delay_days_q95"}},
        },
    }

    KNN = 14
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
    # * K-Nearest Neighbors ----------------------------------------------------
    """
    The K-Nearest Neighbors algorithm computes a distance value for all node
    pairs in the graph and creates new relationships between each node and its
    k nearest neighbors. The distance is calculated based on node properties.
    & 2 960 487 nodes: 7 minutes
    """

    for node in NODE_PROJECTION.keys():
        print(f"Calculate KNN-{KNN} for ({node}) ...")
        rel_type = f"{KNN_REL_NAME}_{node.upper()}"

        # Check existing relationships
        query_string = (
            f"MATCH ()-[r]->() " f"WHERE TYPE(r) = '{rel_type}' " f"RETURN count(r) AS COUNT"
        )
        df = gds.run_cypher(query_string)
        rel_count = df.at[0, "COUNT"]

        if rel_count > 0:
            print(f" # {rel_count:,} relationships [{rel_type}] for ({node})")
            # Delete existing relationships
            print(f"Deleting relationsips [{rel_type}] for ({node})")
            query_string = f"MATCH ()-[r]->() " f"WHERE TYPE(r) = '{rel_type}' " f"DELETE r"
            gds.run_cypher(query_string)
        else:
            print(f" There are no relationships [{rel_type}] for ({node})")

        res = gds.knn.write(
            G,
            topK=KNN,  # of neighbors per nodes
            randomJoins=14,
            sampleRate=0.5,
            deltaThreshold=0.001,
            nodeProperties={
                "delay_days_q95": "COSINE",
                "delay_count_ratio": "COSINE",
                "shipment_count": "JACCARD",
            },
            nodeLabels=[node],
            writeProperty=KNN_PROP_NAME,
            writeRelationshipType=rel_type,
        )
        display(res)
        query_string = (
            f"MATCH (n1:{node})-[r:{rel_type}]->(n2:{node}) "
            f"WHERE r.{KNN_PROP_NAME} IS NOT NULL "
            f"RETURN labels(n1) AS LABEL, "
            f"(CASE WHEN n1.code IS NOT NULL THEN n1.code ELSE n1.name END) AS NAME1, "
            f"(CASE WHEN n2.code IS NOT NULL THEN n2.code ELSE n2.name END) AS NAME2, "
            f"r.{KNN_PROP_NAME} "
            f"ORDER BY r.{KNN_PROP_NAME} ASC "
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
