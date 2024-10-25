-- Создание таблицы transactions для загрузки сырых данных
DROP TABLE IF EXISTS STV2024031225__STAGING.transactions;

CREATE TABLE IF NOT EXISTS STV202406073__STAGING.transactions (
    operation_id UUID,
    account_number_from INT,
    account_number_to INT,
    currency_code INT,
    country VARCHAR(50),
    status VARCHAR(50),
    transaction_type VARCHAR(50),
    amount INT,
    transaction_dt TIMESTAMP,
    
    CONSTRAINT pk_transactions PRIMARY KEY (operation_id)
)
ORDER BY transaction_dt
SEGMENTED BY HASH(operation_id) ALL NODES
PARTITION BY transaction_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(transaction_dt::DATE, 3, 2);