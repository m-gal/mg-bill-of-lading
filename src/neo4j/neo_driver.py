"""
    Neo4j Python Driver custom class and functions to maintain the data
    @author: mikhail.galkin 2022
"""

neo4j_uri = "bolt://localhost:7687"
neo4j_user = "neo4j"
neo4j_pass = "ft-admin"

aws_public_ipv4_address = "18.223.186.49"
aws_neo4j_uri = f"bolt://{aws_public_ipv4_address}:7687"
aws_neo4j_user = "neo4j"
aws_neo4j_pass = "ft-admin"


#%% Setup ----------------------------------------------------------------------
from neo4j import GraphDatabase
from IPython.display import display


#%% Custom classes for python driver -------------------------------------------
class FTNeo4jBolDriver:
    def __init__(self, uri, user, password):
        self.driver = None
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            print(f"Success to create the driver!")
        except Exception as e:
            print(f"Failed to create the driver: {e}")

    def close(self):
        if self.driver is not None:
            self.driver.close()
            print(f"Success to close the driver!")

    def query(self, query, db=None, want_result=None):
        """_summary_

        Args:
            query (_type_): Cypher query string
            db (_type_, optional): _description_. Defaults to None.
            want_result (str, optional): "data" or "graph. Defaults to "data".

        Returns:
            _type_: _description_
        """
        assert self.driver is not None, "Driver not initialized!"
        session = None
        result = None
        try:
            session = (
                self.driver.session(database=db)
                if db is not None
                else self.driver.session()
            )
            result = session.run(query)
        except Exception as e:
            print(f"Query failed: {e}")
        finally:
            if not want_result:
                result = result
            elif want_result == "data":
                result = result.data()
            elif want_result == "graph":
                result = result.graph()
            elif want_result == "values":
                result = result.values()

            if session is not None:
                session.close()

        return result

    def test_connection(self):
        query_string = """
        MATCH (n)
        RETURN DISTINCT labels(n) as Node, count(labels(n)) as Count
        """
        assert self.driver is not None, "Driver not initialized!"
        session = None
        result = None

        try:
            session = self.driver.session()
            result = session.run(query_string)
        except Exception as e:
            print(f"Test failed: {e}")
        finally:
            res = result.data()
            if session is not None:
                session.close()
            print(f"Connection test successful!")
            # display(res)
        return res


#%% Custom functions for python driver -----------------------------------------
def create_fulltext_index_on_node(neo4j_conn, node, props):
    """_summary_

    Args:
        neo4j_conn (_type_): _description_
        node (_type_): _description_
        props (_type_): _description_

    Returns:
        _type_: _description_

    create_fulltext_index_on_node(
        bol, node="Consignee", props=["name", "address_last"]
    )
    create_fulltext_index_on_node(
        bol, node="Shipper", props=["name", "address_last"]
    )
    create_fulltext_index_on_node(
        bol, node="Carrier", props=["code", "name"]
    )
    """
    print(f"Create fulltext index on {node} for {props} ...")
    props = [f"n.{x}" for x in props]
    query_string = (
        f"CREATE FULLTEXT INDEX ftindex{node} IF NOT EXISTS "
        f"FOR (n:{node}) "
        f"ON EACH {props} ".replace("'", "")
    )
    # print(query_string)
    neo4j_conn.query(query_string)
    return True


def create_btree_index_on_node(neo4j_conn, node, prop):
    print(f"Create btree index on {node} for {prop} ...")
    query_string = (
        f"CREATE BTREE INDEX btindex{node} IF NOT EXISTS "
        f"FOR (n:{node}) "
        f"ON (n.{prop[0]})"
    )
    print(query_string)
    neo4j_conn.query(query_string)
    return True


def create_constraint_on_node(neo4j_conn, node, prop):
    print(f"Create unique constraint on {node} for {prop} ...")
    query_string = (
        f"CREATE CONSTRAINT constraint{node} IF NOT EXISTS "
        f"FOR (n:{node}) "
        f"REQUIRE {node}.{prop} IS UNIQUE"
    )
    neo4j_conn.query(query_string)
    return True


def graph_list(neo4j_conn):
    query_string = """
        CALL gds.graph.list()
        YIELD graphName, nodeCount, relationshipCount, creationTime, modificationTime, memoryUsage
        RETURN graphName, nodeCount, relationshipCount, creationTime, modificationTime, memoryUsage
        ORDER BY graphName ASC
        """
    result = neo4j_conn.query(query_string)
    result = [dict(x) for x in result]

    if len(result) == 0:
        print(f"No one graphs exists currently ...")
    else:
        print(result)
    return result


def graph_drop(neo4j_conn, graph_name):
    query_string = f"""
        CALL gds.graph.drop('{graph_name}')
        YIELD graphName
        RETURN graphName
        """
    result = neo4j_conn.query(query_string)
    result = [dict(x) for x in result]
    return result


def check_apoc_pluging(neo4j_conn):
    query_string = f"""
        CALL dbms.procedures()
        YIELD name
        WHERE name STARTS WITH "apoc"
        RETURN count(*) AS count;
        """
    result = neo4j_conn.query(query_string)
    result = [dict(x) for x in result]
    return result
