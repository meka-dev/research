   /* 
CONTEXT: 
- We want to understand how much profit arbitragers are generating on each arbitrage transaction.
- We identify arbitrage transactions as the ones with a trade route of A -> B, B->C, C->A

RESULT
- Each row is a single arbitrage transaction.
- Expected rows: transaction ID, trader, status, arbitrage_token, amount_in, amount_out, profit, ROI, time,total_hops_in_trade

VALIDATION
- Check transaction ID in Mintscan and compare amount_in and amount_out value.
- Additionally you can check the routes to make sure the transaction had 3 trades.

ASSUMPTION:
- Transactions have at least 1 single message with at least 3 trades in it
- Arbitrage trades are only identified at the txn level, which means they all execute in the same block too.
- Cosmos SDK events are fired sequentially
*/
 
-- Profit of arbitraging OSMO trades
select
   * 
from
   (
      with trades as 
      (
         select
            *,
            last_value(is_partition ignore nulls) OVER (partition by tx_id 
         ORDER BY
            rk asc) as trade_index 
         from
            (
               select
                  tx_id,
                  trader,
                  status,
                  event_index,
                  attribute_key,
                  token,
                  safe_cast(amount as int64) as amount,
                  rank() over(partition by tx_id 
               order by
                  rk asc) as rk,
                  first_value(token) over(partition by tx_id 
               order by
                  rk asc) as first_val,
                  last_value(token) over(partition by tx_id 
               order by
                  rk asc) as last_val,
                  case
                     when
                        first_value(token) over(partition by tx_id 
               order by
                  rk asc) = last_value(token) over(partition by tx_id 
               order by
                  rk asc) 
                  and attribute_key = 'tokens_in' 
               then
                  rk 
               else
                  null 
                  end
                  as is_partition, Hours 
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
                           as Status, count(*) as trades 
                        from
                           `immaculate - 355716.osmosis_1.event_attributes` ea 
                           left join
                              `immaculate - 355716.osmosis_1.blocks` b 
                              on b.block_height = cast(ea.block_height as STRING) 
                           inner join
                              `immaculate - 355716.osmosis_1.transactions` t 
                              on t.tx_id = ea.tx_id 
                        where
                           ea.event_type = 'token_swapped' 
                           and t.tx_code = 0 
                           and ea.attribute_key = 'sender' 
                           and block_timestamp >= '2022-07-16'                            -- TEST
                           --and ea.tx_id = '7D0FFEC24B9D438C4886B76C5C0A74653E6646798E5E4DD3BACD641AB2D03E34'
                        group by
                           1, 2                          
                        -- Make sure they have at least 3 trades so ensure txns are arbitrage txns
                        having
                           count(*) >= 3 
                        order by
                           3 desc 
                     )
                     select distinct
                        ea.tx_id TX_ID,
                        ea2.attribute_value as Trader,
                        case
                           when
                              t.tx_code = 0 
                           then
                              'Success' 
                           else
                              'Fail' 
                        end
                        as Status, ea.event_index, ea.attribute_key, 
                        case
                           when
                              ea.attribute_value like '%ibc/%' 
                           then
                              'ibc/' || split(ea.attribute_value, 'ibc/')[offset(ARRAY_LENGTH(split(ea.attribute_value, '/ibc')))] 
                           else
                              'uosmo' 
                        end
                        as token, 
                        case
                           when
                              ea.attribute_value like '%ibc/%' 
                           then
                              split(ea.attribute_value, 'ibc/')[ORDINAL(ARRAY_LENGTH(split(ea.attribute_value, '/ibc')))] 
                           else
                              split(ea.attribute_value, 'uosmo')[offset(0)] 
                        end
                        as amount, rank() over(partition by ea.tx_id 
                     order by
                        ea.event_index, ea.attribute_key) as rk, TIMESTAMP_TRUNC(b.block_timestamp, hour, "UTC") as Hours 
                     from
                        `immaculate - 355716.osmosis_1.event_attributes` ea 
                        inner join
                           `immaculate - 355716.osmosis_1.event_attributes` ea2 
                           on ea2.TX_ID = ea.TX_ID 
                           and ea2.event_index = ea.event_index 
                        inner join
                           `immaculate - 355716.osmosis_1.blocks` b 
                           on b.block_height = cast(ea.block_height as STRING) 
                        inner join
                           `immaculate - 355716.osmosis_1.transactions` t 
                           on t.tx_id = ea.tx_id 
                     where
                        ea.event_type = 'token_swapped' 
                        and ea2.attribute_key = 'sender' 
                        and t.tx_code = 0 
                        and ea.attribute_key in 
                        (
                           'tokens_out', 'tokens_in'
                        )
                        and block_timestamp >= '2022-07-16' 
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
         order by
            rk asc
      )
      select
         t1.tx_id,
         t1.trader,
         t1.status,
         case
            when
               t1.token = 'ibc/27394FB092D2ECCD56123C74F36E4C1F926001CEADA9CA97EA622B25F41E5EB2' 
            then
               'ATOM' 
            when
               t1.token = 'ibc/6AE98883D4D5D5FF9E50D7130F1305DA2FFA0C652D1DD9C123657C6B4EB2DF8A' 
            then
               'EVMOS' 
            when
               t1.token = 'ibc/46B44899322F3CD854D2D46DEEF881958467CDD4B3B10086DA49296BBED94BED' 
            then
               'JUNO' 
            when
               t1.token = 'uosmo' 
            then
               'OSMO' 
            else
               'other' 
         end
         as arbitraged_token, t1.amount as amount_in, t2.amount as amount_out, t2.amount - t1.amount as profit, cast((t2.amount - t1.amount) / t1.amount as float64)*100 as ROI, t1.Hours, 
         (
            t2.rk - t1.rk + 1
         )
          / 2 total_hops_in_trade         --t1.token as A, t3.token as B, t4.token as C, t5.token as D, t6.token as E
      from
         trades t1 
         inner join
            trades t2 
            on t1.tx_id = t2.tx_id 
            and t1.rk < t2.rk 
            and t1.first_val = t2.last_val 
            and t1.trade_index = t2.trade_index             -- todo: remove substraction condition once join is fixed for transactions containing more than one arbitrage for the same coin
      where
         t2.amount - t1.amount > 0 
         and t2.amount - t1.amount is not null 
         and t1.token in 
         (
            'ibc/27394FB092D2ECCD56123C74F36E4C1F926001CEADA9CA97EA622B25F41E5EB2', 'uosmo', 'ibc/6AE98883D4D5D5FF9E50D7130F1305DA2FFA0C652D1DD9C123657C6B4EB2DF8A', 'ibc/46B44899322F3CD854D2D46DEEF881958467CDD4B3B10086DA49296BBED94BED'
         )
         and t1.is_partition is not null 
      order by
         10 desc, 4 asc, 5 asc
   )
where
   -- Make sure each trade has at least 3 hops
   total_hops_in_trade >= 3;