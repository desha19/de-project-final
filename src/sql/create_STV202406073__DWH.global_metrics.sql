DROP TABLE IF EXISTS STV2024031225__DWH.global_metrics;

-- Создание витрины global_metrics с агрегацией по дням
CREATE TABLE IF NOT EXISTS STV202406073__DWH.global_metrics (
    date_update DATE NOT NULL,
    currency_from INT NOT NULL,
    amount_total NUMERIC(18, 2) NOT NULL,
    cnt_transactions INT NOT NULL,
    avg_transactions_per_account NUMERIC(18, 2) NOT NULL,
    cnt_accounts_make_transactions INT NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(50) NOT NULL,
    
    CONSTRAINT pk_global_metrics PRIMARY KEY (date_update, currency_from),
    CONSTRAINT uq_global_metrics_date_update_currency_from UNIQUE (date_update, currency_from),
    CONSTRAINT ch_global_metrics_amount_total CHECK (amount_total >= 0),
    CONSTRAINT ch_global_metrics_cnt_transactions CHECK (cnt_transactions >= 0),
    CONSTRAINT ch_global_metrics_avg_transactions_per_account CHECK (avg_transactions_per_account >= 0),
    CONSTRAINT ch_global_metrics_cnt_accounts_make_transactions CHECK (cnt_accounts_make_transactions >= 0)
)
ORDER BY load_dt
SEGMENTED BY HASH(date_update, currency_from) ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 90, 60);