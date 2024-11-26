WITH supply AS (
    -- First get all minting events (transfers from zero address)
    SELECT DATE_TRUNC('day', evt_block_time) AS Day,
           contract_address,
           SUM(value / POWER(10, 18)) AS Daily
    FROM erc20_ethereum.evt_Transfer
    WHERE contract_address = 0x4c9EDD5852cd905f086C759E8383e09bff1E68B3
    AND "from" = 0x0000000000000000000000000000000000000000
    GROUP BY 1, 2

    UNION ALL

    -- Then get all burning events (transfers to zero address)
    SELECT DATE_TRUNC('day', evt_block_time) AS Day,
           contract_address,
           -SUM(value / POWER(10, 18)) AS Daily  -- Negative for burns
    FROM erc20_ethereum.evt_Transfer
    WHERE contract_address = 0x4c9EDD5852cd905f086C759E8383e09bff1E68B3
    AND "to" = 0x0000000000000000000000000000000000000000
    GROUP BY 1, 2
),

-- Calculate daily net supply changes
daily_supply AS (
    SELECT Day,
           contract_address,
           SUM(Daily) AS Supply
    FROM supply
    GROUP BY 1, 2
),

-- Calculate cumulative supply over time
cumulative_supply AS (
    SELECT Day,
           SUM(Supply) OVER (ORDER BY Day) AS Total
    FROM daily_supply
)

-- Final result ordered by most recent date
SELECT Day,
       Total
FROM cumulative_supply
ORDER BY Day DESC;