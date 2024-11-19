CREATE TEMP FUNCTION findAllInitiators(rootPage STRING, data ARRAY<STRUCT<root_page STRING, third_party STRING, initiator_etld STRING>>)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
  // Helper function to find all initiator_etlds for a given root_page
  function findInitiators(page, visited, data) {
    // Find all entries where the root_page matches and the initiator_etld hasn't been visited
    const initiators = data
      .filter(row => row.root_page === page && !visited.includes(row.initiator_etld))
      .map(row => row.initiator_etld);

    // Add the newly found initiators to the visited list
    visited = visited.concat(initiators);

    // Recursively process all new initiators
    initiators.forEach(initiator => {
      visited = findInitiators(initiator, visited, data);
    });

    return visited;
  }

  // Main call: Start recursion from the rootPage
  // Use a Set to ensure that all returned values are distinct
  return Array.from(new Set(findInitiators(rootPage, [], data)));
""";

with data as (
  -- TP interact with other tps
  SELECT
  *
  FROM (
  SELECT
    client,
    NET.REG_DOMAIN(root_page) root_page,
    NET.REG_DOMAIN(url) third_party,
    NET.REG_DOMAIN(JSON_VALUE(payload, '$._initiator')) initiator_etld
  FROM
    httparchive.all.requests
    #TABLESAMPLE SYSTEM (0.01 PERCENT)
  WHERE
    NET.REG_DOMAIN(root_page) != NET.REG_DOMAIN(url)
    AND date = "2024-06-01")
    WHERE third_party != initiator_etld
    AND root_page != initiator_etld
    group by client, root_page, third_party, initiator_etld
)

SELECT all_initiators as depth, count(*) occ FROM (

SELECT
  root_page,
  client,
  ARRAY_LENGTH(findAllInitiators(root_page, ARRAY_AGG(STRUCT(root_page, third_party, initiator_etld)))) AS all_initiators
FROM data
group by root_page, client) group by all_initiators order by depth

