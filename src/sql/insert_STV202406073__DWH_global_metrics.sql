INSERT INTO STV202406073__DWH.global_metrics
WITH currency_rates AS (
    SELECT * 
    FROM STV202406073__STAGING.currencies c
    WHERE currency_code_with = 420 AND 
          date_update = '{{ ds }}'::date - 1
),
transactions_with_conversion AS (
    SELECT cr.date_update,
           t.currency_code AS currency_from,
           t.account_number_from,
           (t.amount * cr.currency_with_div) AS amount
    FROM STV202406073__STAGING.transactions t
    JOIN currency_rates cr ON t.transaction_dt::date = cr.date_update AND 
         t.currency_code = cr.currency_code
    WHERE t.status = 'done' AND 
          t.account_number_from > 0 AND 
          t.transaction_dt::date = '{{ ds }}'::date - 1
    UNION ALL
    SELECT transaction_dt::date AS date_update,
           currency_code AS currency_from,
           account_number_from,
           amount
    FROM STV202406073__STAGING.transactions
    WHERE currency_code = 420 AND 
          status = 'done' AND
          account_number_from > 0 AND 
          transaction_dt::date = '{{ ds }}'::date - 1
)
SELECT date_update,
       currency_from,
       SUM(amount) AS amount_total,
       COUNT(*) AS cnt_transactions,
       ROUND(SUM(amount) / COUNT(DISTINCT account_number_from), 2) AS avg_transactions_per_account,
       COUNT(DISTINCT account_number_from) AS cnt_accounts_make_transactions,
       '{{ ds }}'::date as load_dt,
       'pg_system_resource' as load_src
FROM transactions_with_conversion
GROUP BY date_update, currency_from;