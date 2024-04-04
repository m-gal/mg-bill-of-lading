//- Count of rels
MATCH ()-[r]->()
RETURN type(r) AS Relationship, count(r) AS count
ORDER BY count DESC
