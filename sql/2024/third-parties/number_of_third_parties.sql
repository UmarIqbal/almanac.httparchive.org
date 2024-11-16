-- Number of first parties with third parties
SELECT
  client,
  AVG(third_partie_requests) mean,
  MIN(third_partie_requests) min,
  MAX(third_partie_requests) max,
  STDDEV(third_partie_requests) sd
FROM (
  SELECT
    client,
    NET.REG_DOMAIN(root_page) root_page,
    COUNT(*) third_partie_requests
  FROM
    httparchive.all.requests
  WHERE
    NET.REG_DOMAIN(root_page) != NET.REG_DOMAIN(url)
    AND date = "2024-06-01"
  GROUP BY
    client,
    root_page
  ORDER BY
    third_partie_requests)
    group by client;

-- Number of first party requests with third parties
SELECT
  client,
  COUNT(*) third_partie_requests
FROM
  httparchive.all.requests
TABLESAMPLE
  SYSTEM (0.1 PERCENT)
WHERE
  NET.REG_DOMAIN(root_page) != NET.REG_DOMAIN(url)
  AND date = "2024-06-01"
GROUP BY
  client;

-- shared bytes with tp
SELECT
  client,
  AVG(content_length) mean,
  MIN(content_length) min,
  MAX(content_length) max,
  STDDEV(content_length) sd
FROM (
SELECT
  client,
  NET.REG_DOMAIN(root_page) root_page,
  SUM(CAST(REGEXP_REPLACE(headers.value, r'[;,]', '') AS FLOAT64)) AS content_length
FROM
  httparchive.all.requests
, UNNEST(response_headers) headers
WHERE
  NET.REG_DOMAIN(root_page) != NET.REG_DOMAIN(url)
  AND date = "2024-06-01"
  AND headers.name = 'Content-Length'
GROUP BY
  client,
  root_page)
  GROUP BY client;
