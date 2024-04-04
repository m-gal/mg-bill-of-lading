"""
    Apply all Graph Data Science algorithms in proper order
    Approximately - 360min

    @author: mikhail.galkin
"""

# %% Setup ----------------------------------------------------------------------
import sys

sys.path.extend([".", "./.", "././.", "..", "../..", "../../.."])

# %% Load project's stuff -------------------------------------------------------
from mgbol.neo4j.xpm.gds.neo_create_indexes import (
    main as neo_create_indexes,
)
from mgbol.neo4j.xpm.gds.gds_centrality_degree import (
    main as gds_centrality_degree,
)
from mgbol.neo4j.xpm.gds.gds_community_wcc import (
    main as gds_community_wcc,
)
from mgbol.neo4j.xpm.gds.gds_community_leiden import (
    main as gds_community_leiden,
)
from mgbol.neo4j.xpm.gds.gds_centrality_pagerank_ports import (
    main as gds_centrality_pagerank_ports,
)
from mgbol.neo4j.xpm.gds.gds_centrality_pagerank_carriers import (
    main as gds_centrality_pagerank_carriers,
)
from mgbol.neo4j.xpm.gds.gds_centrality_pagerank_entities import (
    main as gds_centrality_pagerank_entities,
)
from mgbol.neo4j.xpm.gds.gds_similarity_knn import (
    main as gds_similarity_knn,
)


# %% MAIN -----------------------------------------------------------------------
def main():
    # Create indexes
    neo_create_indexes()

    # Run Centrality Degree algorithm
    gds_centrality_degree()

    # Run Weakly Connected Componenrs algorithm
    gds_community_wcc()

    # Run Leiden Community detection algorithm
    gds_community_leiden()

    # Run Centrality PageRank algorithms for Ports
    gds_centrality_pagerank_ports()

    # Run Centrality PageRank algorithms for Carriers
    gds_centrality_pagerank_carriers()

    # Run Centrality PageRank algorithms for Entities
    gds_centrality_pagerank_entities()

    # Run Similarities algorithm
    gds_similarity_knn()

    print("DONE!")


# %% RUN ========================================================================
if __name__ == "__main__":
    main()
