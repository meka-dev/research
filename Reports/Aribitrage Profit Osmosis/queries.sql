/* 
CONTEXT: 
- We want to understand how much profit arbitragers are generating on each arbitrage transaction.
- We identify arbitrage transactions as the ones with a trade route of A -> B, B->C, C->A
- Curently the query supports only arbitrages with exactly 3 trades.

RESULT
- Each row is a single arbitrage transaction.
- Expected rows: transaction ID, trader, status, arbitrage_token, amount_in, amount_out, profit, ROI, time

VALIDATION
- Check transaction ID in Mintscan and compare amount_in and amount_out value.
- Additionally you can check the routes to make sure the transaction had 3 trades.

ASSUMPTION:
- Transactions have 1 single message with 3 trades in it
- Arbitrage trades are only identified at the txn level, which means they all execute in the same block too.
- Cosmos SDK events are fired sequentially
*/
 
with trades as 
(
   select
      tx_id,
      trader,
      status,
      event_index,
      attribute_key,
      token,
      safe_cast(amount as int64) as amount,
      rk,
      first_value(token) over(partition by tx_id 
   order by
      rk asc) as first,
      last_value(token) over(partition by tx_id 
   order by
      rk asc) as last,
      Time 
   from
      (
         with arbitrage as 
         (
            select
               ea.tx_id TX_ID,
               case
                  when
                     t.tx_code = 0 
                  then
                     'Success' 
                  else
                     'Fail' 
               end
               as Status, count(*) as Trades 
            from
               `immaculate-355716.osmosis_1.event_attributes` ea 
               left join
                  `immaculate-355716.osmosis_1.blocks` b 
                  on b.block_height = cast(ea.block_height as STRING) 
               inner join
                  `immaculate-355716.osmosis_1.transactions` t 
                  on t.tx_id = ea.tx_id 
            where
               ea.event_type = 'token_swapped' 
               and ea.attribute_key = 'sender' 
               and block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 168 Hour) 
            group by
               1, 2 					-- todo: make sure query wors for all trasnactions with more than 3 trades
            having
               count(*) = 3 
            order by
               3 desc 
         )
         select
            ea.tx_id TX_ID,
            ea2.attribute_value as Trader,
            case
               when
                  t.tx_code = 0 
               then
                  'Success' 
               else
                  'Fail' 
            end as Status, 
            ea.event_index, 
            ea.attribute_key, 
            case
               when
                  ea.attribute_value like '%ibc/%' 
               then
                  'ibc/' || split(ea.attribute_value, 'ibc/')[offset(ARRAY_LENGTH(split(ea.attribute_value, '/ibc')))] 
               else
                  'uosmo' 
            end as token, 
            case
               when
                  ea.attribute_value like '%ibc/%' 
               then
                  split(ea.attribute_value, 'ibc/')[ORDINAL(ARRAY_LENGTH(split(ea.attribute_value, '/ibc')))] 
               else
                  split(ea.attribute_value, 'uosmo')[offset(0)] 
            end as amount, 
            rank() over(partition by ea.tx_id 
         		order byea.event_index, ea.attribute_key) as rk, 
            TIMESTAMP_TRUNC(b.block_timestamp, hour, "UTC") as Time, 
         from
            `immaculate-355716.osmosis_1.event_attributes` ea 
            left join
               `immaculate-355716.osmosis_1.event_attributes` ea2 
               on ea2.TX_ID = ea.TX_ID 
               and ea2.event_index = ea.event_index 
               and ea2.event_type = 'token_swapped' 
               and ea2.attribute_key = 'sender' 
            left join
               `immaculate-355716.osmosis_1.blocks` b 
               on b.block_height = cast(ea.block_height as STRING) 
            inner join
               `immaculate-355716.osmosis_1.transactions` t 
               on t.tx_id = ea.tx_id 
         where
            ea.event_type = 'token_swapped' 
            and ea.attribute_key in 
            (
               'tokens_out', 'tokens_in'
            )
            and block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 168 Hour) 
            and ea.tx_id in 
            (
               select distinct
                  tx_id 
               from
                  arbitrage
            )
         order by
            1 desc,
            4 asc,
            5 asc
      )
   order by
      1 desc,
      4 asc,
      5 asc
)
select
   t1.tx_id,
   t1.trader,
   t1.status,
   t1.token as arbitraged_token,
   t1.amount as amount_in,
   t2.amount as amount_out,
   t2.amount-t1.amount as profit,
   cast((t2.amount-t1.amount) / t1.amount as float64)*100 as ROI,
   t1.Time 
from
   trades t1 
   inner join
      trades t2 
      on t1.tx_id = t2.tx_id 
      and t1.rk < t2.rk 
      and t1.first = t2.last 		
-- todo: remove substraction condition once join is fixed for transactions containing more than one arbitrage for the same coin
where
   t1.rk = 1 
   and t2.amount-t1.amount > 0 
   and t2.amount-t1.amount is not null 
   and t1.token = 'uosmo' 
order by
   1 desc,
   4 asc,
   5 asc;