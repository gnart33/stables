-- Define Curve pool addresses and metadata
WITH curve_pools AS (
    SELECT 
        address,
        protocol,
        distinct_name,
        pair
    FROM (VALUES
        (0x02950460e2b9529d0e00284a5fa2d7bdf3fa4d72, 'Curve', 'Curve USDC-USDe', 'USDC-USDe'),
        (0xf36a4ba50c603204c3fc6d2da8b78a7b69cbc67d, 'Curve', 'Curve DAI-USDe', 'DAI-USDe'),
        (0xf55b0f6f2da5ffddb104b58a60f2862745960442, 'Curve', 'Curve crvUSD-USDe', 'crvUSD-USDe'),
        (0x5dc1bf6f1e983c0b21efb003c105133736fa0743, 'Curve', 'Curve FRAX-USDe', 'FRAX-USDe'),
        (0x1ab3d612ea7df26117554dddd379764ebce1a5ad, 'Curve', 'Curve mkUSD-USDe', 'mkUSD-USDe'),
        (0x670a72e6d22b0956c0d2573288f82dcc5d6e3a61, 'Curve', 'Curve GHO-USDe', 'GHO-USDe'),
        (0xf832f4d9087e474357afb9c9a277cb23b9a136cb, 'Curve', 'Curve crvUSD AMM', 'crvUSD-USDe')
    ) AS t(address, protocol, distinct_name, pair)
),

-- Generate hourly time series from start date to now
time_series AS (
    SELECT generate_series(
        TIMESTAMP '2023-11-20 00:00:00',
        CURRENT_TIMESTAMP,
        INTERVAL '1 hour'
    ) AS time
),

-- Calculate inbound and outbound transfers
transfer_events AS (
    -- Inbound transfers
    SELECT 
        date_trunc('hour', evt_block_time) AS time,
        t.to AS address,
        value/1e18 AS amount
    FROM erc20_ethereum.evt_Transfer t
    WHERE t.evt_block_time >= DATE '2023-11-20'
        AND t.contract_address = 0x4c9edd5852cd905f086c759e8383e09bff1e68b3
        AND t.to != 0x0000000000000000000000000000000000000000
    
    UNION ALL
    
    -- Outbound transfers (negative amounts)
    SELECT 
        date_trunc('hour', evt_block_time) AS time,
        t."from" AS address,
        -value/1e18 AS amount
    FROM erc20_ethereum.evt_Transfer t
    WHERE t.evt_block_time >= DATE '2023-11-20'
        AND t.contract_address = 0x4c9edd5852cd905f086c759e8383e09bff1e68b3
        AND t."from" != 0x0000000000000000000000000000000000000000
),

-- Aggregate transfers and calculate TVL
transfers AS (
    SELECT 
        ts.time,
        cp.address,
        cp.protocol,
        cp.distinct_name,
        cp.pair,
        COALESCE(SUM(te.amount), 0) AS flow,
        SUM(COALESCE(SUM(te.amount), 0)) OVER (
            PARTITION BY cp.address 
            ORDER BY ts.time
        ) AS tvl
    FROM time_series ts
    CROSS JOIN curve_pools cp
    LEFT JOIN transfer_events te ON 
        te.time = ts.time 
        AND te.address = cp.address
    GROUP BY 1, 2, 3, 4, 5
)

-- Final result
SELECT 
    time,
    address,
    flow,
    tvl,
    protocol,
    distinct_name,
    pair
FROM transfers
ORDER BY time, pool;