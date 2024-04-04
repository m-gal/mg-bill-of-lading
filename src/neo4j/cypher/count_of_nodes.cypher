//- Count of nodes
MATCH (n)
RETURN labels(n) AS Label, count(n) AS count
ORDER BY count DESC
