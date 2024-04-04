"""
    Prepares CSV files for bulk import into Neo4j GraphDB
    Approximately - 360min

    @author: mikhail.galkin
"""

# %% Setup ----------------------------------------------------------------------
import sys

sys.path.extend([".", "./.", "././.", "..", "../..", "../../.."])

# %% Load project's stuff -------------------------------------------------------
from mgbol.neo4j.neo_driver import FTNeo4jBolDriver
from mgbol.neo4j.neo_driver import neo4j_uri
from mgbol.neo4j.neo_driver import neo4j_user
from mgbol.neo4j.neo_driver import neo4j_pass
from mgbol.neo4j.neo_driver import create_fulltext_index_on_node
from mgbol.neo4j.neo_driver import create_btree_index_on_node
from mgbol.neo4j.neo_driver import create_constraint_on_node


# %% MAIN -----------------------------------------------------------------------
def main():

    FULLTEXT_INDEX_SPEC = {
        "Consignee": ["name"],
        "Shipper": ["name"],
        "Carrier": ["name", "name_full"],
        "NotifyParty": ["name"],
        "PortOfLading": ["name"],
        "PortOfUnlading": ["name"],
    }

    BTREE_INDEX_SPEC = {
        "Consignee": ["_prank_weighted"],
        "Shipper": ["_prank_weighted"],
        "Carrier": ["_prank_weighted"],
        "NotifyParty": ["_prank_weighted"],
        "PortOfLading": ["_prank_weighted"],
        "PortOfUnlading": ["_prank_weighted"],
    }

    CONSTRAINT_SPEC = {
        "Consignee": ["name"],
        "Shipper": ["name"],
        "Carrier": ["code"],
        "PortOfLading": ["code"],
        "PortOfUnlading": ["code"],
    }

    bol = FTNeo4jBolDriver(neo4j_uri, neo4j_user, neo4j_pass)
    bol.test_connection()

    # Create full text index in Nodes if not exists
    for node, props in FULLTEXT_INDEX_SPEC.items():
        create_fulltext_index_on_node(bol, node, props)

    # # Create btree index in Nodes if not exists
    # for node, props in BTREE_INDEX_SPEC.items():
    #     create_btree_index_on_node(bol, node, props)

    # # Create unique constraints for Nodes
    # for node, props in CONSTRAINT_SPEC.items():
    #     create_constraint_on_node(bol, node, props)

    # # to use for weighted pagerank for RELATIONSHIP
    # print(f"Create the inverted 'delay_days_q95' property ....................")
    # print(f"\tfor [r: 'SHIPS_FOR', 'SHIPS_BY', 'CARRIES_FOR'] ...")

    # rels = ["SHIPS_FOR", "SHIPS_BY", "CARRIES_FOR", "LADING_IN", "UNLADING_IN"]
    # for rel in rels:
    #     query_string = (
    #         f"MATCH ()-[r]->() "
    #         f"WHERE type(r) '{rel}' "
    #         f"AND r.delay_days_q95 IS NOT NULL "
    #         f"WITH r, "
    #         f"CASE WHEN (r.delay_days_q95 <= 0) THEN 1 ELSE (1/toFloat(r.delay_days_q95)) END AS d "
    #         f"SET r.delay_days_q95i = d"
    #     )
    #     bol.query(query_string)

    bol.close()
    print("Done.")


# %% RUN ========================================================================
if __name__ == "__main__":
    main()
