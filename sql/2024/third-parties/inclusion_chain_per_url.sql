-- Assuming 'trigger_urls' is the table containing the 100 URLs we are interested in
-- Structure: trigger_urls(trigger_url STRING)

-- Creating the recursive UDF with depth tracking for each third_party and initiator_etld
CREATE TEMP FUNCTION findTriggerUrlsWithDepth(rootPage STRING, triggerUrls ARRAY<STRING>, data ARRAY<STRUCT<root_page STRING, third_party STRING, initiator_etld STRING>>)
RETURNS ARRAY<STRUCT<trigger_url STRING, depth INT64>>
LANGUAGE js AS """
  // Helper function to find all initiator_etlds for a given root_page and track their depth
  function findInitiators(page, visited, depthMap, currentDepth, data) {
    // Find all entries where the root_page matches and the initiator_etld hasn't been visited
    const initiators = data
      .filter(row => row.root_page === page && !visited.includes(row.initiator_etld))
      .map(row => row.initiator_etld);

    // Track depth for each initiator and match with triggerUrls
    initiators.forEach(initiator => {
      if (!depthMap[initiator]) {
        depthMap[initiator] = [];
      }
      depthMap[initiator].push(currentDepth);
    });

    // Add the newly found initiators to the visited list
    visited = visited.concat(initiators);

    // Recursively process all new initiators
    initiators.forEach(initiator => {
      findInitiators(initiator, visited, depthMap, currentDepth + 1, data);
    });
  }

  // Main call: Initialize depthMap and visited list, and start recursion from rootPage
  const depthMap = {};
  findInitiators(rootPage, [], depthMap, 1, data);

  // Filter the depthMap to only include trigger URLs and flatten the depths for each
  const result = [];
  triggerUrls.forEach(triggerUrl => {
    if (depthMap[triggerUrl]) {
      depthMap[triggerUrl].forEach(depth => {
        result.push({ trigger_url: triggerUrl, depth: depth });
      });
    }
  });

  return result;
""";

-- Example: Using the function for all pages
WITH trigger_urls AS (
  -- Replace this with the actual query or table for your 100 trigger URLs
  SELECT root_page as trigger_url
  FROM `httparchive._1b0e99cf071c648e157dc1c2bb8a2f1b817751b9.anon4704d19f38341c9ef96f1ec24e0b04644233cac5c776f34906233b682d3a26ef`
),

data as (
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
),

data_aggregated AS (
  -- Aggregate the data for each root_page
  SELECT
    root_page,
    ARRAY_AGG(STRUCT(NET.REG_DOMAIN(root_page), NET.REG_DOMAIN(url), NET.REG_DOMAIN(JSON_VALUE(payload, '$._initiator')))) AS data_array
  FROM
    httparchive.all.requests
    #TABLESAMPLE SYSTEM (0.01 PERCENT)
  WHERE
    NET.REG_DOMAIN(root_page) != NET.REG_DOMAIN(url)
    AND date = "2024-06-01")
    WHERE third_party != initiator_etld
    AND root_page != initiator_etld
    group by client, root_page
),

trigger_depths AS (
  -- Run recursive analysis for each root_page and collect depths for trigger URLs
  SELECT
    root_page,
    trigger_result.trigger_url,
    trigger_result.depth
  FROM data_aggregated
  CROSS JOIN (SELECT ARRAY_AGG(trigger_url) AS trigger_urls FROM trigger_urls) trigger_data
  CROSS JOIN UNNEST(findTriggerUrlsWithDepth(root_page, trigger_data.trigger_urls, data_array)) AS trigger_result
)

-- Calculate the average depth for each trigger URL
SELECT
  trigger_url,
  AVG(depth) AS average_depth
FROM trigger_depths
GROUP BY trigger_url
ORDER BY average_depth;
