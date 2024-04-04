"""

    @author: mikhail.galkin 2022
"""

#%% Setup ----------------------------------------------------------------------
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

URI = "bolt://localhost:11003"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "ft-admin"

# driver = GraphDatabase.driver(URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
# print(driver.verify_connectivity())


#%% Custom classes -------------------------------------------------------------
class FTNeo4jBolInject:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

        # # Create index on resource type
        # with self.driver.session() as session:
        #     # Create index on node:Company.name to facilitate rapid upsert
        #     session.write_transaction(self._create_btreeindex_on_consignee_name)

    def close(self):
        self.driver.close()

    def delete_all_nodes_and_relations(self):
        with self.driver.session() as session:
            session.write_transaction(self._delete_all_nodes_and_relations)

    @staticmethod
    def _delete_all_nodes_and_relations(tx):
        tx.run("MATCH (n) DETACH DELETE n ")

    def delete_nodes_and_relations(self, nodes: list):
        with self.driver.session() as session:
            for node in nodes:
                session.write_transaction(
                    self._delete_some_nod_and_relations, node=node
                )
                print(f"Node '{node}': DELETED")

    @staticmethod
    def _delete_some_nod_and_relations(tx, node):
        tx.run(f"MATCH (n: {node}) DETACH DELETE n ")

    # Find Consignee
    def find_consignee(self, consignee_name):
        with self.driver.session() as session:
            result = session.read_transaction(
                self._find_and_return_consignee,
                consignee_name,
            )
            for row in result:
                print(f"Found consignee: {row}")

    @staticmethod
    def _find_and_return_consignee(tx, consignee_name):
        result = tx.run(
            "MATCH (c:Consignee) "
            f"WHERE c.name = '{consignee_name}' "
            "RETURN c.name AS consignee "
        )
        return [row["consignee"] for row in result]

    # * Shipper - SHIPS_FOR -> Consignee
    def create_ships_for(self, shipper, consignee):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_and_return_ships_for, shipper, consignee
            )
            for row in result:
                print(
                    f"Created SHIPS_FOR between: {row['shipper']} -> {row['consignee']}"
                )

    @staticmethod
    def _create_and_return_ships_for(tx, shipper, consignee):
        result = tx.run(
            f"MERGE (a:Shipper {{name: '{shipper}'}}) "
            f"MERGE (b:Consignee {{name: '{consignee}'}}) "
            "MERGE (a)-[r:SHIPS_FOR]->(b) "
            "RETURN a, b, type(r) "
        )
        return [
            {
                "shipper": row["a"]["name"],
                "consignee": row["b"]["name"],
                "relation": row["type(r)"],
            }
            for row in result
        ]

    # * Shipper - SHIPS_WITH -> Shipment
    def create_ships_with(self, shipper, shipment):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_and_return_ships_with, shipper, shipment
            )
            for row in result:
                print(
                    f"Created SHIPS_WITH between: {row['shipper']} -> {row['shipment']}"
                )

    @staticmethod
    def _create_and_return_ships_with(tx, shipper, shipment):
        result = tx.run(
            f"MERGE (a:Shipper {{name: '{shipper}'}}) "
            f"MERGE (b:Shipment {{name: '{shipment}'}}) "
            "MERGE (a)-[r:SHIPS_WITH]->(b) "
            "RETURN a, b, type(r) "
        )
        return [
            {
                "shipper": row["a"]["name"],
                "shipment": row["b"]["name"],
                "relation": row["type(r)"],
            }
            for row in result
        ]

    # * Shipper - SHIPS_BY -> Carrier
    def create_ships_by(self, shipper, carrier):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_and_return_ships_by, shipper, carrier
            )
            for row in result:
                print(
                    f"Created SHIPS_BY between: {row['shipper']} -> {row['carrier']}"
                )

    @staticmethod
    def _create_and_return_ships_by(tx, shipper, carrier):
        result = tx.run(
            f"MERGE (a:Shipper {{name: '{shipper}'}}) "
            f"MERGE (b:Carrier {{name: '{carrier}'}}) "
            "MERGE (a)-[r:SHIPS_BY]->(b) "
            "RETURN a, b, type(r) "
        )
        return [
            {
                "shipper": row["a"]["name"],
                "carrier": row["b"]["name"],
                "relation": row["type(r)"],
            }
            for row in result
        ]

    # * Shipper - SHIPS_TO -> PortOfUnlading
    def create_ships_to(self, shipper, port_of_unlading):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_and_return_ships_to, shipper, port_of_unlading
            )
            for row in result:
                print(
                    f"Created SHIPS_TO between: {row['shipper']} -> {row['port_of_unlading']}"
                )

    @staticmethod
    def _create_and_return_ships_to(tx, shipper, port_of_unlading):
        result = tx.run(
            f"MERGE (a:Shipper {{name: '{shipper}'}}) "
            f"MERGE (b:PortOfUnlading {{name: '{port_of_unlading}'}}) "
            "MERGE (a)-[r:SHIPS_TO]->(b) "
            "RETURN a, b, type(r) "
        )
        return [
            {
                "shipper": row["a"]["name"],
                "port_of_unlading": row["b"]["name"],
                "relation": row["type(r)"],
            }
            for row in result
        ]

    # * Shipper - SHIPS_FROM -> PortOfLading
    def create_ships_from(self, shipper, port_of_lading):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_and_return_ships_from, shipper, port_of_lading
            )
            for row in result:
                print(
                    f"Created SHIPS_FROM between: {row['shipper']} -> {row['port_of_lading']}"
                )

    @staticmethod
    def _create_and_return_ships_from(tx, shipper, port_of_lading):
        result = tx.run(
            f"MERGE (a:Shipper {{name: '{shipper}'}}) "
            f"MERGE (b:PortOfLading {{name: '{port_of_lading}'}}) "
            "MERGE (a)-[r:SHIPS_FROM]->(b) "
            "RETURN a, b, type(r) "
        )
        return [
            {
                "shipper": row["a"]["name"],
                "port_of_lading": row["b"]["name"],
                "relation": row["type(r)"],
            }
            for row in result
        ]

    # * Carrier - CARRIES_WITH -> Shipment
    def create_carries_with(self, carrier, shipment):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_and_return_carries_with, carrier, shipment
            )
            for row in result:
                print(
                    f"Created CARRIES_WITH between: {row['carrier']} -> {row['shipment']}"
                )

    @staticmethod
    def _create_and_return_carries_with(tx, carrier, shipment):
        result = tx.run(
            f"MERGE (a:Carrier {{name: '{carrier}'}}) "
            f"MERGE (b:Shipment {{name: '{shipment}'}}) "
            "MERGE (a)-[r:CARRIES_WITH]->(b) "
            "RETURN a, b, type(r) "
        )
        return [
            {
                "carrier": row["a"]["name"],
                "shipment": row["b"]["name"],
                "relation": row["type(r)"],
            }
            for row in result
        ]

    # * Carrier - CARRIES_BY -> VESSEL
    def create_carries_by(self, carrier, vessel):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_and_return_carries_by, carrier, vessel
            )
            for row in result:
                print(
                    f"Created CARRIES_BY between: {row['carrier']} -> {row['vessel']}"
                )

    @staticmethod
    def _create_and_return_carries_by(tx, carrier, vessel):
        result = tx.run(
            f"MERGE (a:Carrier {{name: '{carrier}'}}) "
            f"MERGE (b:Vessel {{name: '{vessel}'}}) "
            "MERGE (a)-[r:CARRIES_BY]->(b) "
            "RETURN a, b, type(r) "
        )
        return [
            {
                "carrier": row["a"]["name"],
                "vessel": row["b"]["name"],
                "relation": row["type(r)"],
            }
            for row in result
        ]

    # * Vessel - VESSEL_WITH -> Shipment
    def create_vessel_with(self, vessel, shipment):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_and_return_vessel_with, vessel, shipment
            )
            for row in result:
                print(
                    f"Created VESSEL_WITH between: {row['vessel']} -> {row['shipment']}"
                )

    @staticmethod
    def _create_and_return_vessel_with(tx, vessel, shipment):
        result = tx.run(
            f"MERGE (a:Vessel {{name: '{vessel}'}}) "
            f"MERGE (b:Shipment {{name: '{shipment}'}}) "
            "MERGE (a)-[r:VESSEL_WITH]->(b) "
            "RETURN a, b, type(r) "
        )
        return [
            {
                "vessel": row["a"]["name"],
                "shipment": row["b"]["name"],
                "relation": row["type(r)"],
            }
            for row in result
        ]

    # * Vessel - VESSEL_FOR -> Consignee
    def create_vessel_for(self, vessel, consignee):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_and_return_vessel_for, vessel, consignee
            )
            for row in result:
                print(
                    f"Created VESSEL_FOR between: {row['vessel']} -> {row['consignee']}"
                )

    @staticmethod
    def _create_and_return_vessel_for(tx, vessel, consignee):
        result = tx.run(
            f"MERGE (a:Vessel {{name: '{vessel}'}}) "
            f"MERGE (b:Consignee {{name: '{consignee}'}}) "
            "MERGE (a)-[r:VESSEL_FOR]->(b) "
            "RETURN a, b, type(r) "
        )
        return [
            {
                "vessel": row["a"]["name"],
                "consignee": row["b"]["name"],
                "relation": row["type(r)"],
            }
            for row in result
        ]

    # * Shipment - SHIPMENT_FOR -> Consignee
    def create_shipment_for(self, shipment, consignee):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_and_return_shipment_for, shipment, consignee
            )
            for row in result:
                print(
                    f"Created SHIPMENT_FOR between: {row['shipment']} -> {row['consignee']}"
                )

    @staticmethod
    def _create_and_return_shipment_for(tx, shipment, consignee):
        result = tx.run(
            f"MERGE (a:Shipment {{name: '{shipment}'}}) "
            f"MERGE (b:Consignee {{name: '{consignee}'}}) "
            "MERGE (a)-[r:SHIPMENT_FOR]->(b) "
            "RETURN a, b, type(r) "
        )
        return [
            {
                "shipment": row["a"]["name"],
                "consignee": row["b"]["name"],
                "relation": row["type(r)"],
            }
            for row in result
        ]

    # * Shipment - SHIPMENT_TO -> PortOfUnlading
    def create_shipment_to(self, shipment, port_of_unlading):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_and_return_shipment_to, shipment, port_of_unlading
            )
            for row in result:
                print(
                    f"Created SHIPMENT_TO between: {row['shipment']} -> {row['port_of_unlading']}"
                )

    @staticmethod
    def _create_and_return_shipment_to(tx, shipment, port_of_unlading):
        result = tx.run(
            f"MERGE (a:Shipment {{name: '{shipment}'}}) "
            f"MERGE (b:PortOfUnlading {{name: '{port_of_unlading}'}}) "
            "MERGE (a)-[r:SHIPMENT_TO]->(b) "
            "RETURN a, b, type(r) "
        )
        return [
            {
                "shipment": row["a"]["name"],
                "port_of_unlading": row["b"]["name"],
                "relation": row["type(r)"],
            }
            for row in result
        ]

    # * Shipment - SHIPMENT_FROM -> PortOfLading
    def create_shipment_from(self, shipment, port_of_lading):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_and_return_shipment_from, shipment, port_of_lading
            )
            for row in result:
                print(
                    f"Created SHIPMENT_FROM between: {row['shipment']} -> {row['port_of_lading']}"
                )

    @staticmethod
    def _create_and_return_shipment_from(tx, shipment, port_of_lading):
        result = tx.run(
            f"MERGE (a:Shipment {{name: '{shipment}'}}) "
            f"MERGE (b:PortOfLading {{name: '{port_of_lading}'}}) "
            "MERGE (a)-[r:SHIPMENT_FROM]->(b) "
            "RETURN a, b, type(r) "
        )
        return [
            {
                "shipment": row["a"]["name"],
                "port_of_lading": row["b"]["name"],
                "relation": row["type(r)"],
            }
            for row in result
        ]

    # * Shipment - SHIPMENT_IN -> Container
    def create_shipment_in(self, shipment, container):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_and_return_shipment_in, shipment, container
            )
            for row in result:
                print(
                    f"Created SHIPMENT_IN between: {row['shipment']} -> {row['container']}"
                )

    @staticmethod
    def _create_and_return_shipment_in(tx, shipment, container):
        result = tx.run(
            f"MERGE (a:Shipment {{name: '{shipment}'}}) "
            f"MERGE (b:Container {{name: '{container}'}}) "
            "MERGE (a)-[r:SHIPMENT_IN]->(b) "
            "RETURN a, b, type(r) "
        )
        return [
            {
                "shipment": row["a"]["name"],
                "container": row["b"]["name"],
                "relation": row["type(r)"],
            }
            for row in result
        ]

    # * Shipment - SHIPMENT_BOL -> BillOfLading
    def create_shipment_bol(self, shipment, bill_of_lading):
        with self.driver.session() as session:
            result = session.write_transaction(
                self._create_and_return_shipment_bol, shipment, bill_of_lading
            )
            for row in result:
                print(
                    f"Created SHIPMENT_BOL between: {row['shipment']} -> {row['bill_of_lading']}"
                )

    @staticmethod
    def _create_and_return_shipment_bol(tx, shipment, bill_of_lading):
        result = tx.run(
            f"MERGE (a:Shipment {{name: '{shipment}'}}) "
            f"MERGE (b:BillOfLading {{name: '{bill_of_lading}'}}) "
            "MERGE (a)-[r:SHIPMENT_BOL]->(b) "
            "RETURN a, b, type(r) "
        )
        return [
            {
                "shipment": row["a"]["name"],
                "bill_of_lading": row["b"]["name"],
                "relation": row["type(r)"],
            }
            for row in result
        ]


#%% RUN ========================================================================
if __name__ == "__main__":
    shipper = "Shipper"
    consignee = "Consignee"
    shipment = "Shipment"
    carrier = "Carrier"
    vessel = "Vessel"
    container = "Container"
    port_of_lading = "Port of Lading"
    port_of_unlading = "Port of Unlading"
    bill_of_lading = "Bill of Lading"

    bol = FTNeo4jBolInject(URI, NEO4J_USER, NEO4J_PASSWORD)

    bol.create_ships_for(shipper=shipper, consignee=consignee)
    bol.create_ships_with(shipper=shipper, shipment=shipment)
    bol.create_ships_by(shipper=shipper, carrier=carrier)
    bol.create_ships_to(shipper=shipper, port_of_unlading=port_of_unlading)
    bol.create_ships_from(shipper=shipper, port_of_lading=port_of_lading)
    bol.create_carries_with(carrier=carrier, shipment=shipment)
    bol.create_carries_by(carrier=carrier, vessel=vessel)
    bol.create_vessel_with(vessel=vessel, shipment=shipment)
    bol.create_vessel_for(vessel=vessel, consignee=consignee)
    bol.create_shipment_for(shipment=shipment, consignee=consignee)
    bol.create_shipment_to(shipment=shipment, port_of_unlading=port_of_unlading)
    bol.create_shipment_from(shipment=shipment, port_of_lading=port_of_lading)
    bol.create_shipment_in(shipment=shipment, container=container)
    bol.create_shipment_bol(shipment=shipment, bill_of_lading=bill_of_lading)

    bol.close()
