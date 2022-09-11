-- Queries templates for Bigquery
-- Fees by transaction types
with fees as (SELECT 
	tx_id,
	attribute_value
	FROM `immaculate-355716.osmosis_1.event_attributes` 
where event_type = 'tx'
and attribute_key = 'fee'
order by 2 desc)
select
	case when ea.attribute_value like '%ibc%' then 'IBC ' || split(ea.attribute_value, 'Msg')[ORDINAL(ARRAY_LENGTH(split(ea.attribute_value, 'Msg')))] else split(ea.attribute_value, 'Msg')[ORDINAL(ARRAY_LENGTH(split(ea.attribute_value, 'Msg')))] end as "Transaction Type",
  	sum(safe_cast(REGEXP_REPLACE(f.attribute_value, r'\..*|[^0-9]', '') as int64))/1000000 as Fees
FROM `immaculate-355716.osmosis_1.event_attributes` ea
	inner join fees f on f.tx_id = ea.tx_id
	inner join `immaculate-355716.osmosis_1.blocks` b on b.block_height = cast(ea.block_height as STRING)
where event_type = 'message' 
  	and attribute_key = 'action'
	and DATE(b.block_timestamp) between '2022-07-16' and current_date('UTC')
group by 1
order by 2 desc;

-- Count of unique transactions by message/action type
select
	case when attribute_value like '%ibc%' then 'IBC ' || split(attribute_value, 'Msg')[ORDINAL(ARRAY_LENGTH(split(attribute_value, 'Msg')))] else split(attribute_value, 'Msg')[ORDINAL(ARRAY_LENGTH(split(attribute_value, 'Msg')))] end as Transaction,
 	count(distinct(tx_id)) as Transactions
FROM `immaculate-355716.osmosis_1.event_attributes`
where event_type = 'message' 
  and attribute_key = 'action'
group by 1
order by 2 desc;

-- Delegations over time by delegator
select
	TIMESTAMP_TRUNC(b.block_timestamp, hour, "UTC") as hours,
	ea2.attribute_value,
	sum(safe_cast(REGEXP_REPLACE(ea.attribute_value, r'\..*|[^0-9]', '') as int64))/1000000 as osmo_delegations
from `immaculate-355716.osmosis_1.event_attributes` ea
left join `immaculate-355716.osmosis_1.blocks` b on b.block_height = cast(ea.block_height as STRING)
left join `immaculate-355716.osmosis_1.event_attributes` ea2 on ea2.tx_id = ea.tx_id and ea2.attribute_key = 'validator'
where ea.event_type = 'delegate'
and ea2.event_type = 'delegate'
and ea.attribute_key = 'amount'
and block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
group by 1,2
order by 1 asc;

-- Trades by Pool ID over time
select
  count(*) as Trades, 
  TIMESTAMP_TRUNC(b.block_timestamp, hour, "UTC") as Hours,
  attribute_value as Pool_ID
from `immaculate-355716.osmosis_1.event_attributes` ea
left join `immaculate-355716.osmosis_1.blocks` b on b.block_height = cast(ea.block_height as STRING)
where event_type = 'token_swapped'
and attribute_key = 'pool_id'
and block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
group by 2,3 order by 2 desc;

--Trades by Trader
select
	ea2.attribute_value as Trader,
	count(distinct(ea.tx_id)) as Trades
from `immaculate-355716.osmosis_1.event_attributes` ea
left join `immaculate-355716.osmosis_1.blocks` b on b.block_height = cast(ea.block_height as STRING)
left join `immaculate-355716.osmosis_1.event_attributes` ea2 on ea2.tx_id = ea.tx_id and ea2.attribute_key = 'sender'
where ea.event_type = 'token_swapped'
and ea2.event_type = 'token_swapped'
and ea.attribute_key = 'pool_id'
and block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 Hour)
group by 1
order by 2 desc;

-- Txns with arbitrage trades
-- Trades by Trader
select
	ea.tx_id TX_ID,
	ea.attribute_value as Trader,
	case when t.tx_code = 0 then 'Success' else 'Fail' end as Status,
	count(*) as Trades
from `immaculate-355716.osmosis_1.event_attributes` ea
left join `immaculate-355716.osmosis_1.blocks` b on b.block_height = cast(ea.block_height as STRING)
inner join `immaculate-355716.osmosis_1.transactions` t on t.tx_id = ea.tx_id
where ea.event_type = 'token_swapped'
and ea.attribute_key = 'sender'
and block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 Hour)
group by 1,2,3
order by 4 desc;

-- Profit of arbitraging osmos trades with 3 trade cycle
-- Trades by Trader -> test: FFF8D4C1AE5031125E5FA21A03F4D44B30C0931A47FE9A51676D165E0BB4BB56
-- Trades by Trader -> test: FFF8D4C1AE5031125E5FA21A03F4D44B30C0931A47FE9A51676D165E0BB4BB56
with trades as (select
	tx_id, trader, status, event_index, attribute_key, token, safe_cast(amount as int64) as amount, rk,
	first_value(token) over(partition by tx_id order by rk asc) as first,
	last_value(token) over(partition by tx_id order by rk asc) as last,
	Hours
	from
(with arbitrage as (
	select
	ea.tx_id TX_ID,
	case when t.tx_code = 0 then 'Success' else 'Fail' end as Status,
	count(*) as Trades
from `immaculate-355716.osmosis_1.event_attributes` ea
left join `immaculate-355716.osmosis_1.blocks` b on b.block_height = cast(ea.block_height as STRING)
inner join `immaculate-355716.osmosis_1.transactions` t on t.tx_id = ea.tx_id
where ea.event_type = 'token_swapped'
and ea.attribute_key = 'sender'
and block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 168 Hour)
group by 1,2
-- todo: make sure query wors for all trasnactions with more than 3 trades
having count(*) = 3
order by 3 desc
)
select
	ea.tx_id TX_ID,
	ea2.attribute_value as Trader,
	case when t.tx_code = 0 then 'Success' else 'Fail' end as Status,
	ea.event_index,
	ea.attribute_key,
	case 
	when ea.attribute_value like '%ibc/%' then 'ibc/' || split(ea.attribute_value, 'ibc/')[offset(ARRAY_LENGTH(split(ea.attribute_value, '/ibc')))] 
 	else 'uosmo'
	end as token,
	case 
	when ea.attribute_value like '%ibc/%' then split(ea.attribute_value, 'ibc/')[ORDINAL(ARRAY_LENGTH(split(ea.attribute_value, '/ibc')))] 
 	else split(ea.attribute_value, 'uosmo')[offset(0)]
	end as amount,
	rank() over(partition by ea.tx_id order by ea.event_index, ea.attribute_key) as rk,
	TIMESTAMP_TRUNC(b.block_timestamp, hour, "UTC") as Hours,
from `immaculate-355716.osmosis_1.event_attributes` ea
left join `immaculate-355716.osmosis_1.event_attributes` ea2 on ea2.TX_ID = ea.TX_ID and ea2.event_index = ea.event_index and ea2.event_type = 'token_swapped' and ea2.attribute_key = 'sender'
left join `immaculate-355716.osmosis_1.blocks` b on b.block_height = cast(ea.block_height as STRING)
inner join `immaculate-355716.osmosis_1.transactions` t on t.tx_id = ea.tx_id
where ea.event_type = 'token_swapped'
and ea.attribute_key in ('tokens_out', 'tokens_in')
and block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 168 Hour)
and ea.tx_id in (select distinct tx_id from arbitrage)
order by 1 desc, 4 asc, 5 asc)
order by 1 desc, 4 asc, 5 asc)
select
	t1.tx_id, t1.trader, t1.status, t1.token as arbitraged_token,
	t1.amount as amount_in,
	t2.amount as amount_out,
	t2.amount - t1.amount as profit,
	cast((t2.amount - t1.amount)/t1.amount as float64)*100 as ROI,
	t1.Hours
from trades t1 inner join trades t2 on t1.tx_id = t2.tx_id and t1.rk < t2.rk and t1.first = t2.last
-- todo: remove substraction condition once join is fixed for transactions containing more than one arbitrage for the same coin
where t1.rk = 1 and t2.amount - t1.amount > 0 and t2.amount - t1.amount is not null
and t1.token = 'uosmo'
order by 1 desc, 4 asc, 5 asc;

SELECT t0.frequency, t0.path FROM (
select 
  action_1 || ' -> ' || action_2 as path,
  count(*) as frequency
from
(with transactions as (select
  ea.tx_id,
  ea.event_index,
  block_timestamp,
  split(ea.attribute_value, 'Msg')[ORDINAL(ARRAY_LENGTH(split(ea.attribute_value, 'Msg')))] as action,
  split(ea2.attribute_value, '/')[ordinal(1)] as sender,
  rank() over(partition by split(ea2.attribute_value, '/')[ordinal(1)] order by block_timestamp asc, ea.tx_id asc, ea.event_index asc) as sequence,
  case when ea.attribute_value like '%MsgWithdrawDelegatorReward' then split(ea.attribute_value, 'Msg')[ORDINAL(ARRAY_LENGTH(split(ea.attribute_value, 'Msg')))] else null end as is_rewards,
  --case when ea.attribute_value like '%MsgWithdrawDelegatorReward' then ea.event_index else null end as event_index_grp
from `immaculate-355716.osmosis_1.event_attributes` ea
inner join `immaculate-355716.osmosis_1.event_attributes` ea2 on ea.tx_id = ea2.tx_id and ea2.attribute_key = 'acc_seq'
inner join `immaculate-355716.osmosis_1.blocks` b on ea.block_height = cast(b.block_height as int64)
--where ea.tx_id in ('30E687DD3A4D469D95A6D20DEC986CBB27CD815953219E8C86647389226C642B','FD41A3627E0CADD5F3EEECFFC8FCA259250D060FC4713270EFB3E4D2D6CD454C')
and ea.event_type = 'message'
and ea.attribute_key = 'action'
and block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
order by 6 asc, ea.event_index asc)
select 
  t1.tx_id,
  t1.sender,
  t1.action as action_1,
  t2.action as action_2
from transactions t1
inner join transactions t2 on t1.sender = t2.sender and t2.sequence = t1.sequence + 1 and t1.action <> t2.action
where t1.action = 'WithdrawDelegatorReward'
order by sender, t1.sequence asc)
group by 1
order by 2 desc
) AS t0 LIMIT 100; 

-- Arbitrage trades
-- Profit of arbitraging OSMO trades 136756
select * from (with trades as (select
  *,
  last_value(is_partition ignore nulls) OVER (partition by tx_id ORDER BY rk asc) as trade_index
	from (
	select
	tx_id, trader, status, event_index, attribute_key, token, safe_cast(amount as int64) as amount, rank() over(partition by tx_id order by rk asc) as rk,
	first_value(token) over(partition by tx_id order by rk asc) as first_val,
	last_value(token) over(partition by tx_id order by rk asc) as last_val,
	case 
		when first_value(token) over(partition by tx_id order by rk asc) = last_value(token) over(partition by tx_id order by rk asc) and attribute_key = 'tokens_in' then rk
		else null end as is_partition,
	Hours
	from
(with arbitrage as (
	select
	ea.tx_id TX_ID,
	case when t.tx_code = 0 then 'Success' else 'Fail' end as Status,
	count(*) as trades
from `immaculate-355716.osmosis_1.event_attributes` ea
left join `immaculate-355716.osmosis_1.blocks` b on b.block_height = cast(ea.block_height as STRING)
inner join `immaculate-355716.osmosis_1.transactions` t on t.tx_id = ea.tx_id
where ea.event_type = 'token_swapped'
and t.tx_code = 0
and ea.attribute_key = 'sender'
and block_timestamp >= '2022-07-16'
-- TEST
--and ea.tx_id = '7D0FFEC24B9D438C4886B76C5C0A74653E6646798E5E4DD3BACD641AB2D03E34'
group by 1,2
-- Select number of minimal hops in this case it's 3
having count(*) >= 3
order by 3 desc
)
select
  distinct
	ea.tx_id TX_ID,
	ea2.attribute_value as Trader,
	case when t.tx_code = 0 then 'Success' else 'Fail' end as Status,
	ea.event_index,
	ea.attribute_key,
	case 
	when ea.attribute_value like '%ibc/%' then 'ibc/' || split(ea.attribute_value, 'ibc/')[offset(ARRAY_LENGTH(split(ea.attribute_value, '/ibc')))] 
 	else 'uosmo'
	end as token,
	case 
	when ea.attribute_value like '%ibc/%' then split(ea.attribute_value, 'ibc/')[ORDINAL(ARRAY_LENGTH(split(ea.attribute_value, '/ibc')))] 
 	else split(ea.attribute_value, 'uosmo')[offset(0)]
	end as amount,
	rank() over(partition by ea.tx_id order by ea.event_index, ea.attribute_key) as rk,
	TIMESTAMP_TRUNC(b.block_timestamp, hour, "UTC") as Hours
from `immaculate-355716.osmosis_1.event_attributes` ea
inner join `immaculate-355716.osmosis_1.event_attributes` ea2 on ea2.TX_ID = ea.TX_ID and ea2.event_index = ea.event_index
inner join `immaculate-355716.osmosis_1.blocks` b on b.block_height = cast(ea.block_height as STRING)
inner join `immaculate-355716.osmosis_1.transactions` t on t.tx_id = ea.tx_id
where ea.event_type = 'token_swapped'
and ea2.attribute_key = 'sender'
and t.tx_code = 0
and ea.attribute_key in ('tokens_out', 'tokens_in')
and block_timestamp >= '2022-07-16'
and ea.tx_id in (select distinct tx_id from arbitrage)
order by 1 desc, 4 asc, 5 asc)
order by 1 desc, 4 asc, 5 asc)
order by rk asc)
select
	t1.tx_id, 
	t1.trader, 
	t1.status, 
	case 
		when t1.token = 'ibc/27394FB092D2ECCD56123C74F36E4C1F926001CEADA9CA97EA622B25F41E5EB2' then 'ATOM' 
		when t1.token = 'ibc/6AE98883D4D5D5FF9E50D7130F1305DA2FFA0C652D1DD9C123657C6B4EB2DF8A' then 'EVMOS'
		when t1.token = 'ibc/46B44899322F3CD854D2D46DEEF881958467CDD4B3B10086DA49296BBED94BED' then 'JUNO'
		when t1.token = 'uosmo' then 'OSMO'
		else 'other'
	end as arbitraged_token,
	t1.amount as amount_in,
	t2.amount as amount_out,
	t2.amount - t1.amount as profit,
	cast((t2.amount - t1.amount)/t1.amount as float64)*100 as ROI,
	t1.Hours,
	(t2.rk - t1.rk + 1)/2 total_hops_in_trade
  --t1.token as A, t3.token as B, t4.token as C, t5.token as D, t6.token as E
from trades t1 inner join trades t2 on t1.tx_id = t2.tx_id and t1.rk < t2.rk and t1.first_val = t2.last_val and t1.trade_index = t2.trade_index
-- todo: remove substraction condition once join is fixed for transactions containing more than one arbitrage for the same coin
where t2.amount - t1.amount > 0 and t2.amount - t1.amount is not null
and t1.token in ('ibc/27394FB092D2ECCD56123C74F36E4C1F926001CEADA9CA97EA622B25F41E5EB2', 'uosmo', 'ibc/6AE98883D4D5D5FF9E50D7130F1305DA2FFA0C652D1DD9C123657C6B4EB2DF8A','ibc/46B44899322F3CD854D2D46DEEF881958467CDD4B3B10086DA49296BBED94BED')
and t1.is_partition is not null
order by 10 desc, 4 asc, 5 asc)
where total_hops_in_trade >= 3;
