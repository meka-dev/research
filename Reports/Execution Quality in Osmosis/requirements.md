# Execution Quality

## Motivation

A key metric for exchanges (such as Osmosis) is the quality of execution they provide for their users.  It’s often argued that the worst kind of MEV (Sandwich attacks) hurts users and execution quality is the measure of that pain. Recent research [suggest](https://t.co/UmKbBhL2FM) the opposite, that MEV (in particular cases) can actually improve execution. at Mekatek we are  trying to find the balance between opportunity (maximising revenue) and utility (execution quality). In order to have meaningful conversations with key stake holders (Osmosis, validators, users) we need to understand what is going on quantitatively. In short, we need to understand the execution quality on osmosis and how (if at all) is being impacted by Sandwich attacks.

## Definitions

- Slippage (**MS**):
    - given an executed (successful)
        - asset_a ⇒ asset_b with input (amount) `a_i`
        - pool AB with reserves `a_r` and `b_r`
        - exchange rate `x_ab` = `a_r` / `b_r`
        - post swap exchange rate `x_ab'`
    - Slippage **S**
    
    ```matlab
    S = (x_ab' - x_ab) / x_ab
    ```
    
- Total Slippage (**TS**):
    - the amount of slippage of all transactions
- Materialized Slippage (**MS**)
    - The amount of slippage of successful transactions
- Avoided Slippage (**AS**)
    - The amount of slippage of failed transaction
- Sandwich attack (**SA)** be a sequence of transactions

```matlab
- account_x: asset_a => asset_b
- account_y : asset_a => asset_b
- account_x: asset_b => asset_a
```

- Swap Sequence (**SS**)
    - A sequence of swap transactions (same account, same pair)
- Swap Sequence Cost (**SSC**)
    - The sum of fees paid by transactions (failed and executed) in the **SS**
- Retried Transactions (**RT**)
    - The failed Transactions within a **SS**
- Total Virtual Slippage (**TVS**)
    - Given an **SS.** **TVS** = the difference between the exchange rate of the initial **RT** in the ordered **SS** and  ****the exchange rate of the ****final transaction in the **SS**
- Execution Quality:
    - quoted price: (relative to some index)
    - speed of execution
    - likelihood of trade executions
    - all measurements bucketed by size of trade
        - 0-10 USD
        - 10-100 USD
        - 100-1000 USD
        - …

## Questions

- What is the total amount of slippage (TS, MS, AS)
- How much slippage comes from sandwich attack like sequences
- What is the distribution of sandwich attacks across block proposers
- What is the distribution of fees across blocks with sandwich attack
- How many Swap Sequences succeed / fail
    - What is the cost **SSC**
    - What is the **TVS**
- How often are traders getting the most optimal price?
- Average slippage per user
- How many transactions does it take for a user to successfully submit their trade
- What is the ratio slippage / total volume?