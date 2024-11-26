/* combined */
/* ORDER BY day ASC, */
/*     CASE WHEN address = 'others' THEN 1 ELSE 0 END, */
/*     running_balance DESC */
WITH transfers AS (
  SELECT
    DATE_TRUNC('day', evt_block_time) AS day,
    "from",
    "to",
    SUM(TRY_CAST(value AS DOUBLE) / POWER(10, 18)) AS total_value,
    COUNT(*) AS number_of_transfers
  FROM ethena_labs_ethereum.usde_evt_transfer
  GROUP BY
    1,
    2,
    3
), daily_net_value AS (
  SELECT
    day,
    address,
    SUM(CASE WHEN address = t."to" THEN total_value ELSE -total_value END) AS net_value
  FROM (
    SELECT
      day,
      "from" AS address,
      total_value,
      "to"
    FROM transfers
    UNION ALL
    SELECT
      day,
      "to" AS address,
      total_value,
      "to"
    FROM transfers
  ) AS t
  GROUP BY
    1,
    2
), dates AS (
  SELECT
    day
  FROM UNNEST(SEQUENCE(
    TRY_CAST((
      SELECT
        MIN(day)
      FROM daily_net_value
    ) AS TIMESTAMP),
    CAST(TRY_CAST((
      SELECT
        MAX(day)
      FROM daily_net_value
    ) AS TIMESTAMP) AS TIMESTAMP),
    INTERVAL '1' DAY
  ) /* WARNING: Check out the docs for example of time series generation: https://dune.com/docs/query/syntax-differences/ */
  /* WARNING: Check out the docs for example of time series generation: https://dune.com/docs/query/syntax-differences/ */) AS _u(day)
), daily_value_changes AS (
  SELECT
    d.day,
    a.address,
    COALESCE(dnv.net_value, 0) AS daily_net_value
  FROM dates AS d
  CROSS JOIN (
    SELECT DISTINCT
      address
    FROM daily_net_value
  ) AS a
  LEFT JOIN daily_net_value AS dnv
    ON d.day = dnv.day AND a.address = dnv.address
), top_holders AS (
  SELECT
    day,
    address,
    SUM(daily_net_value) OVER (PARTITION BY address ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
  FROM (
    SELECT
      d.day,
      TRY_CAST(th.address AS VARCHAR) AS address,
      SUM(dvc.daily_net_value) AS daily_net_value
    FROM dates AS d
    CROSS JOIN (
      SELECT
        address
      FROM daily_value_changes
      GROUP BY
        address
      ORDER BY
        SUM(daily_net_value) DESC
      LIMIT 10
    ) AS th
    LEFT JOIN daily_value_changes AS dvc
      ON d.day = dvc.day AND dvc.address = th.address
    GROUP BY
      d.day,
      th.address
    UNION ALL
    SELECT
      d.day,
      'others' AS address,
      SUM(dvc.daily_net_value) AS daily_net_value
    FROM dates AS d
    LEFT JOIN daily_value_changes AS dvc
      ON d.day = dvc.day
      AND NOT dvc.address IN (
        SELECT
          address
        FROM daily_value_changes
        GROUP BY
          address
        ORDER BY
          SUM(daily_net_value) DESC
        LIMIT 10
      )
      AND dvc.address <> 0x0000000000000000000000000000000000000000 /* Exclude zero address */
    GROUP BY
      d.day
  )
)

SELECT
    th.*,
    COALESCE(cm.contract_name, 'unlabeled') AS contract_name,
    COALESCE(cm.contract_project, 'unlabeled') AS contract_project
FROM
    top_holders th
    LEFT JOIN contracts.contract_mapping cm ON th.address = CAST(cm.contract_address AS varchar)
WHERE
    th.day >= CURRENT_DATE - INTERVAL '30' DAY
    AND
    cm.contract_name IS NOT NULL