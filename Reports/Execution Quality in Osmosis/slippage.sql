with slippage as (select
  *,
  no_slippage_amount - amount_out as slippage_in_token_out,
  case
    when native_denom_out <> 'OSMO' then (no_slippage_amount - amount_out) / price
    else no_slippage_amount - amount_out
  end as slippage_in_osmo
from 
(select 
  *, 
  LEAD(price) OVER(order by block_timestamp desc, tx_index desc, event_index desc) as previous_price,
  case 
    when native_denom_in = 'OSMO' then  amount_in * LEAD(price) OVER(order by block_timestamp desc, tx_index desc, event_index desc)
    else amount_in / (LEAD(price) OVER(order by block_timestamp desc, tx_index desc, event_index desc))
  end as no_slippage_amount
  from (select
  block_timestamp,
  s.block_height,
  s.tx_id,
  tx_index,
  native_denom_in,
  native_denom_out,
  parsed_amount_in as amount_in,
  event_index,
  case 
    when native_denom_in = 'OSMO' then parsed_amount_out / parsed_amount_in 
    else parsed_amount_in / parsed_amount_out
  end as price,
  parsed_amount_out as amount_out
from `immaculate-355716.osmosis_1.swaps` s
inner join `immaculate-355716.osmosis_1.transactions` t on t.tx_id = s.tx_id
where pool_id = '678'
and date(block_timestamp) between '2022-10-01' and '2022-10-25'
order by 1 desc, 4 desc)
order by block_height desc))
select
  *
from slippage
order by 1 desc, 4 desc;