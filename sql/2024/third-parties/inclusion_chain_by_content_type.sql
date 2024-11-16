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
    NET.REG_DOMAIN(JSON_VALUE(payload, '$._initiator')) initiator_etld,
    response_header.value as content_type
  FROM
    httparchive.all.requests,
    UNNEST(response_headers) response_header
    #TABLESAMPLE SYSTEM (0.01 PERCENT)
  WHERE
    NET.REG_DOMAIN(root_page) != NET.REG_DOMAIN(url)
    AND date = "2024-06-01"
    AND response_header.name = 'content-type'
    )
    WHERE third_party != initiator_etld
    AND root_page != initiator_etld
    group by client, root_page, third_party, initiator_etld, content_type
)

SELECT
client,
content_type,
AVG(all_initiators) mean,
  MIN(all_initiators) min,
  MAX(all_initiators) max,
  STDDEV(all_initiators) sd
  FROM (
SELECT
  root_page,
  client,
  content_type,
  ARRAY_LENGTH(findAllInitiators(root_page, ARRAY_AGG(STRUCT(root_page, third_party, initiator_etld)))) AS all_initiators
FROM data
group by root_page, client, content_type
) group by client, content_type;






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
    NET.REG_DOMAIN(JSON_VALUE(payload, '$._initiator')) initiator_etld,
    response_header.value as content_type,
    type
  FROM
    httparchive.all.requests,
    UNNEST(response_headers) response_header
    #TABLESAMPLE SYSTEM (0.01 PERCENT)
  WHERE
    NET.REG_DOMAIN(root_page) != NET.REG_DOMAIN(url)
    AND date = "2024-06-01"
    AND response_header.name = 'content-type'
    )
    WHERE third_party != initiator_etld
    AND root_page != initiator_etld
    group by client, root_page, third_party, initiator_etld, content_type, type
)

SELECT distinct client, type, AVG(mean) mean FROM (
SELECT
client,
type,
AVG(all_initiators) mean,
  MIN(all_initiators) min,
  MAX(all_initiators) max,
  STDDEV(all_initiators) sd
  FROM (
SELECT
  root_page,
  client,
    #CONCAT(SPLIT(content_type, '/')[SAFE_OFFSET(0)], '/', SPLIT(content_type, '/')[SAFE_OFFSET#(1)])AS top_content_type,
    type,
  ARRAY_LENGTH(findAllInitiators(root_page, ARRAY_AGG(STRUCT(root_page, third_party, initiator_etld)))) AS all_initiators
FROM data
group by root_page, client, content_type, type
) group by client, type) GROUP BY client, type order by mean desc;
