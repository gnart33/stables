WITH time_series AS (
    SELECT time
    FROM (
    unnest(sequence(CAST('2023-11-20 00:00' AS timestamp), CAST(NOW() AS timestamp), interval '1' hour)) AS s(time)
    )
    )

, staked_usde AS (
    SELECT time
    , SUM(staked) AS staked
    FROM (
        SELECT ts.time
        , SUM(COALESCE(SUM(t.assets/1e18), 0)) OVER (ORDER BY ts.time) AS staked
        FROM time_series ts
        LEFT JOIN ethena_labs_ethereum.StakedUSDeV2_evt_Deposit t ON ts.time=date_trunc('hour', t.evt_block_time)
        GROUP BY 1
        
        UNION ALL
        
        SELECT ts.time
        , SUM(-COALESCE(SUM(t.assets/1e18), 0)) OVER (ORDER BY ts.time) AS staked
        FROM time_series ts
        LEFT JOIN ethena_labs_ethereum.StakedUSDeV2_evt_Withdraw t ON ts.time=date_trunc('hour', t.evt_block_time)
        GROUP BY 1
        )
    GROUP BY 1
    )

, usde_supply AS (
    SELECT ts.time
    , SUM(SUM(COALESCE(CASE WHEN t."from"=0x0000000000000000000000000000000000000000 THEN t.value/1e18 ELSE -t.value/1e18 END, 0))) OVER (ORDER BY ts.time) AS supply
    FROM time_series ts
    LEFT JOIN ethena_labs_ethereum.USDe_evt_Transfer t ON ts.time=date_trunc('hour', t.evt_block_time)
        AND (t."from" = 0x0000000000000000000000000000000000000000 
        OR t.to = 0x0000000000000000000000000000000000000000)
    GROUP BY 1
    )

SELECT CAST(supply.time AS timestamp) AS time
, 'raw' AS held_as
, supply.supply-staked.staked AS value
, supply.supply AS supply_only
, NULL AS staked_only
FROM usde_supply supply
LEFT JOIN staked_usde staked ON staked.time=supply.time

UNION ALL

SELECT CAST(time AS timestamp) AS time
, 'staked' AS held_as
, staked AS value
, NULL AS supply_only
, staked AS staked_only
FROM staked_usde staked