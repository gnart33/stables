with transfers as (
    select 
        date_trunc('day', evt_block_time) as day,
        "from",
        to,
        sum(value / POWER(10, 18)) AS total_value,
        count(*) as number_of_transfers
    from ethena_labs_ethereum.USDe_evt_Transfer
    group by 1, 2, 3
)
select 
    day,
    address,
    sum(case 
        when address = to then value
        else -value
    end) as net_value
from (
    -- Outgoing transfers (negative value)
    select day, "from" as address, total_value as value
    from transfers
    union all
    -- Incoming transfers (positive value)
    select day, to as address, total_value as value
    from transfers
) combined
group by 1, 2
order by day desc, net_value desc