DROP TABLE IF EXISTS STV2024031225__STAGING.currencies;

-- Создание таблицы currencies для загрузки сырых данных
CREATE TABLE IF NOT EXISTS STV202406073__STAGING.currencies (
    date_update TIMESTAMP,
    currency_code INT,
    currency_code_with INT,
    currency_with_div NUMERIC(18, 2),
    
    CONSTRAINT pk_currencies PRIMARY KEY (date_update, currency_code, currency_code_with)
)
ORDER BY date_update
SEGMENTED BY HASH(date_update, currency_code, currency_code_with) ALL NODES
PARTITION BY date_update::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(date_update::DATE, 90, 60);
