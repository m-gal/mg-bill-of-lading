//- Index of nodes
MATCH (n)
RETURN ID(n) AS id, n.name AS name
ORDER BY id DESC LIMIT 10
