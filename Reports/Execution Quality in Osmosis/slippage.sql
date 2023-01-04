   /* 
CONTEXT: 
- We want to extract transactions that get sandwiched in Osmosis
- A sandwiched transaction in this context is identified as a sequence of trades for the same (partitioned by) pool where:
- account_x: asset_a => asset_b
- account_y: asset_a => asset_b
- account_x: asset_b => asset_a
- NOTE: A sandwiched transaction is not necessarily a sandwiched attack. See article.

RESULT
- Each row is a single trade including the previous and next trades to check if it's been in_between_same trader.
- We also calculate slippage a posteriori, as we don't have access to the liquidity of the pools at trading time. This has some limitations liquidity can be taken out in between trades, but it should be a good proxy for understanding execution quality.

VALIDATION
- Check transaction ID in Mintscan and check thta the previous and next trade on that pool correspond to the same ones in the table.

ASSUMPTION:
- Pools are partitioned by pool
- We use lead and lag to get last and next trader
- We ignore other potential events like liquidity provision, join_pool, etc.

*/

WITH sandwiched AS
(
       SELECT                    *,
              no_slippage_amount - amount_out AS slippage_in_token_out,
              CASE
                     WHEN native_denom_out <> 'OSMO' THEN (no_slippage_amount - amount_out) / price
                     ELSE no_slippage_amount - amount_out
              END AS slippage_in_osmo
       FROM   (
                       SELECT   *,
                                Lead(price) OVER(partition BY pool_id ORDER BY Cast(pool_id AS INT64) DESC, block_timestamp DESC, tx_index DESC, event_index DESC) AS previous_price,
                                CASE
                                         WHEN native_denom_in = 'OSMO' THEN amount_in * Lead(price) OVER(partition BY pool_id ORDER BY Cast(pool_id AS INT64) DESC, block_timestamp DESC, tx_index DESC, event_index DESC)
                                         ELSE amount_in                               / Lead(price) OVER(partition BY pool_id ORDER BY Cast(pool_id AS INT64) DESC, block_timestamp DESC, tx_index DESC, event_index DESC)
                                END AS no_slippage_amount
                       FROM     (
                                           SELECT     s.block_timestamp,
                                                      Cast(s.block_height AS INT64) AS block_height,
                                                      s.tx_id,
                                                      s.sender,
                                                      tx_index,
                                                      native_denom_in,
                                                      native_denom_out,
                                                      parsed_amount_in AS amount_in,
                                                      event_index,
                                                      pool_id,
                                                      CASE
                                                                 WHEN native_denom_in = 'OSMO' THEN parsed_amount_in
                                                                 ELSE parsed_amount_out
                                                      END AS volume_in_osmo,
                                                      CASE
                                                                 WHEN native_denom_in = 'OSMO' THEN parsed_amount_out / parsed_amount_in
                                                                 ELSE parsed_amount_in                                / parsed_amount_out
                                                      END               AS price,
                                                      parsed_amount_out AS amount_out
                                           FROM       `immaculate-355716.osmosis_1.swaps` s
                                           INNER JOIN
                                                      (
                                                                      SELECT DISTINCT tx_id,
                                                                                      tx_index
                                                                      FROM            `immaculate-355716.osmosis_1.transactions`) t
                                           ON         t.tx_id = s.tx_id
                                           WHERE
                                                      CASE
                                                                 WHEN native_denom_in = 'OSMO' THEN parsed_amount_in
                                                                 ELSE parsed_amount_out
                                                      END > 1
                                           AND        pool_id IN ('1',
                                                                  '678',
                                                                  '704',
                                                                  '712',
                                                                  '722',
                                                                  '674',
                                                                  '604',
                                                                  '497',
                                                                  '9',
                                                                  '812',
                                                                  '584',
                                                                  '3',
                                                                  '481',
                                                                  '42',
                                                                  '463',
                                                                  '15',
                                                                  '730',
                                                                  '577',
                                                                  '641',
                                                                  '3',
                                                                  '816',
                                                                  '806',
                                                                  '813')
                                           ORDER BY   pool_id DESC,
                                                      block_timestamp DESC,
                                                      tx_index DESC)
                       ORDER BY cast(block_height AS int64) DESC))
SELECT   cast(block_height AS int64) AS block_height,
         date(block_timestamp)       AS date,
         tx_id,
         tx_index,
         sender,
         native_denom_in,
         native_denom_out,
         volume_in_osmo,
         slippage_in_osmo,
         pool_id,
         lag(sender) OVER(partition BY pool_id ORDER BY cast(block_height AS int64) DESC, tx_index DESC)  next_sender,
         lead(sender) OVER(partition BY pool_id ORDER BY cast(block_height AS int64) DESC, tx_index DESC) previous_sender,
         CASE
                  WHEN lag(sender) OVER(partition BY pool_id ORDER BY cast(block_height AS int64) DESC, tx_index DESC) = lead(sender) OVER(partition BY pool_id ORDER BY cast(block_height AS int64) DESC, tx_index DESC) THEN 'TRUE'
         END AS in_between_same_trader,
         CASE
                  WHEN (
                                    lag(sender) OVER(partition BY pool_id ORDER BY cast(block_height AS int64) DESC, tx_index DESC) = lead(sender) OVER(partition BY pool_id ORDER BY cast(block_height AS int64) DESC, tx_index DESC))
                  AND      (
                                    lag(native_denom_out) OVER(partition BY pool_id ORDER BY cast(block_height AS int64) DESC, tx_index DESC) <> native_denom_out)
                  AND      (
                                    lead(native_denom_out) OVER(partition BY pool_id ORDER BY cast(block_height AS int64) DESC, tx_index DESC) = native_denom_out)
                  AND      sender <> lag(sender) OVER(partition BY pool_id ORDER BY cast(block_height AS int64) DESC, tx_index DESC)
                  AND      sender <> lead(sender) OVER(partition BY pool_id ORDER BY cast(block_height AS int64) DESC, tx_index DESC) THEN 'TRUE'
         END AS sandwiched_transaction
FROM     sandwiched
ORDER BY cast(pool_id AS int64) ASC,
         1 DESC,
         2 DESC,
         3 DESC;