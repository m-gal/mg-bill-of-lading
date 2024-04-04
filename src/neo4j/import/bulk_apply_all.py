"""
    Prepares CSV files for bulk import into Neo4j GraphDB
    Approximately - 360min

    @author: mikhail.galkin
"""

# %% Setup ----------------------------------------------------------------------
import sys

sys.path.extend([".", "./.", "././.", "..", "../..", "../../.."])

# %% Load project's stuff -------------------------------------------------------
from mgbol.neo4j.xpm.bulk_import.bulk_node_name_n_attribute import (
    main as bulk_node_name_n_attribute,
)
from mgbol.neo4j.xpm.bulk_import.bulk_node_ports import (
    main as bulk_node_ports,
)
from mgbol.neo4j.xpm.bulk_import.bulk_rel_carrier_consignee import (
    main as bulk_rel_carrier_consignee,
)
from mgbol.neo4j.xpm.bulk_import.bulk_rel_carrier_portoflading import (
    main as bulk_rel_carrier_portoflading,
)
from mgbol.neo4j.xpm.bulk_import.bulk_rel_carrier_portofunlading import (
    main as bulk_rel_carrier_portofunlading,
)
from mgbol.neo4j.xpm.bulk_import.bulk_rel_consignee_portoflading import (
    main as bulk_rel_consignee_portoflading,
)
from mgbol.neo4j.xpm.bulk_import.bulk_rel_consignee_portofunlading import (
    main as bulk_rel_consignee_portofunlading,
)
from mgbol.neo4j.xpm.bulk_import.bulk_rel_notifyparty_consignee import (
    main as bulk_rel_notifyparty_consignee,
)
from mgbol.neo4j.xpm.bulk_import.bulk_rel_notifyparty_portofunlading import (
    main as bulk_rel_notifyparty_portofunlading,
)
from mgbol.neo4j.xpm.bulk_import.bulk_rel_shipper_carrier import (
    main as bulk_rel_shipper_carrier,
)
from mgbol.neo4j.xpm.bulk_import.bulk_rel_shipper_consignee import (
    main as bulk_rel_shipper_consignee,
)
from mgbol.neo4j.xpm.bulk_import.bulk_rel_shipper_portoflading import (
    main as bulk_rel_shipper_portoflading,
)
from mgbol.neo4j.xpm.bulk_import.bulk_rel_shipper_portofunlading import (
    main as bulk_rel_shipper_portofunlading,
)


# %% MAIN -----------------------------------------------------------------------
def main():
    # Creates Data & Header CSV for Nodes: Consignee \ Shipper \ NotifyParty
    _ = bulk_node_name_n_attribute()

    # Creates Data & Header CSV for Nodes: PortOfLading \ PortOfUnlading
    _ = bulk_node_ports()

    # Relationship: (Carrier) - [CARRIES_FOR] -> (Consignee)
    bulk_rel_carrier_consignee()

    # Relationship: (Carrier) - [CARRIES_FROM] -> (PortOfLading)
    bulk_rel_carrier_portoflading()

    # Relationship: (Carrier) - [CARRIES_TO] -> (PortOfUnlading)
    bulk_rel_carrier_portofunlading()

    # Relationship: (PortOfLading) - [LADING_FOR] -> (Consignee)
    bulk_rel_consignee_portoflading()

    # Relationship: (PortOfUnlading) - [UNLADING_FOR] -> (Consignee)
    bulk_rel_consignee_portofunlading()

    # Relationship: (NotifyParty) - [NOTIFY_FOR] -> (Consignee)
    bulk_rel_notifyparty_consignee()

    # Relationship: (NotifyParty) - [NOTIFY_IN] -> (PortOfUnlading)
    bulk_rel_notifyparty_portofunlading()

    # Relationship: (Shipper) - [SHIPS_BY] -> (Carrier)
    bulk_rel_shipper_carrier()

    # Relationship: (Shipper) - [SHIPS_FOR] -> (Consignee)
    bulk_rel_shipper_consignee()

    # Relationship: (Shipper) - [SHIPS_FROM] -> (PortOfLading)
    bulk_rel_shipper_portoflading()

    # Relationship: (Shipper) - [SHIPS_TO] -> (PortOfUnlading)
    bulk_rel_shipper_portofunlading()

    print("DONE!")


# %% RUN ========================================================================
if __name__ == "__main__":
    main()
