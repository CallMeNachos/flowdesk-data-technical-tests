WITH result AS (
    -- Get the closest timestamp ranking
	SELECT
		t.transaction_id,
        t.exchange,
        t.exchange_type,
        t.account_id,
        t.client_id,
        t.currency_1,
        t.currency_2,
        t.from_address,
        t.to_address,
        t.executed_at,
        t.size,
        t.price,
		i.updated_at,
		ROW_NUMBER() OVER(
		    PARTITION BY
		        t.executed_at, t.exchange, t.exchange_type, t.currency_1, t.currency_2
		    ORDER BY
		        ABS(EXTRACT(EPOCH FROM (t.executed_at - i.updated_at)))
        ) AS rn
	FROM
		trades t
	LEFT JOIN
		indexes i
	ON
		t.exchange = i.exchange
		AND t.exchange_type = i.exchange_type
		AND t.currency_1 = i.currency_1
		AND t.currency_2 = i.currency_2
)

-- Keep top 1 rank only
SELECT
    transaction_id,
    exchange,
    exchange_type,
    account_id,
    client_id,
    currency_1,
    currency_2,
    from_address,
    to_address,
    executed_at,
    size,
    price,
    updated_at
FROM
    result
WHERE
    rn = 1;