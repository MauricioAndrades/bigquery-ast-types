MERGE `event_analytics.order` AS T USING (
  WITH deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        retailer_id,
        order_id,
        COALESCE(NULLIF(TRIM(child_product_id), ''), CAST(product_id AS STRING)),
        session_id,
        visitor_id,
        crm_id,
        platform,
        user_ip,
        zip_code,
        hashed_email,
        ARRAY_TO_STRING(
          ARRAY(
            SELECT CONCAT(pii.key, ':', pii.value)
            FROM UNNEST(hashed_pii) AS pii
            WHERE pii.key IS NOT NULL
              AND pii.value IS NOT NULL
            ORDER BY pii.key, pii.value
          ),
          ', '
        ),
        product_id,
        quantity,
        regular_unit_price,
        discount_unit_price,
        merchant_id,
        country,
        is_restaurant,
        is_recurring,
        user_tracking_allowed,
        seller_id
      ORDER BY
        order_ts ASC,
        symbiosys_ts ASC
    ) AS rn
  FROM `symbiosys-prod.event_staging.order`
  WHERE order_ts >= TIMESTAMP('2025-07-01 00:00:00')
    AND order_ts < TIMESTAMP('2025-08-01 00:00:00')
)
SELECT * FROM deduped
) AS B
ON
  -- retailer and order identifiers
  T.retailer_id = B.retailer_id
  AND (T.order_id = B.order_id OR (T.order_id IS NULL AND B.order_id IS NULL))

  -- timestamps
  AND (T.order_ts = B.order_ts OR (T.order_ts IS NULL AND B.order_ts IS NULL))

  -- all the rest of the scalar fields, null‑safe
  AND (T.session_id = B.session_id OR (T.session_id IS NULL AND B.session_id IS NULL))
  AND (T.visitor_id = B.visitor_id OR (T.visitor_id IS NULL AND B.visitor_id IS NULL))
  AND (T.crm_id = B.crm_id OR (T.crm_id IS NULL AND B.crm_id IS NULL))
  AND (T.platform = B.platform OR (T.platform IS NULL AND B.platform IS NULL))
  AND (T.user_ip = B.user_ip OR (T.user_ip IS NULL AND B.user_ip IS NULL))
  AND (T.zip_code = B.zip_code OR (T.zip_code IS NULL AND B.zip_code IS NULL))
  AND (T.hashed_email = B.hashed_email OR (T.hashed_email IS NULL AND B.hashed_email IS NULL))

  -- inline, sorted fingerprint of the PII array
  AND (
    ARRAY_TO_STRING(
      ARRAY(
        SELECT CONCAT(x.key, ':', x.value)
        FROM UNNEST(T.hashed_pii) AS x
        WHERE x.key IS NOT NULL
          AND x.value IS NOT NULL
        ORDER BY x.key, x.value
      ),
      ', '
    )
    =
    ARRAY_TO_STRING(
      ARRAY(
        SELECT CONCAT(x.key, ':', x.value)
        FROM UNNEST(B.hashed_pii) AS x
        WHERE x.key IS NOT NULL
          AND x.value IS NOT NULL
        ORDER BY x.key, x.value
      ),
      ', '
    )
  )

  AND (T.product_id = B.product_id OR (T.product_id IS NULL AND B.product_id IS NULL))
  AND (T.quantity = B.quantity OR (T.quantity IS NULL AND B.quantity IS NULL))
  AND (T.regular_unit_price = B.regular_unit_price OR (T.regular_unit_price IS NULL AND B.regular_unit_price IS NULL))
  AND (T.discount_unit_price = B.discount_unit_price OR (T.discount_unit_price IS NULL AND B.discount_unit_price IS NULL))
  AND (T.merchant_id = B.merchant_id OR (T.merchant_id IS NULL AND B.merchant_id IS NULL))
  AND (T.country = B.country OR (T.country IS NULL AND B.country IS NULL))

  -- boolean flags with null-safe comparison
  AND (T.is_restaurant = B.is_restaurant OR (T.is_restaurant IS NULL AND B.is_restaurant IS NULL))
  AND (T.is_recurring = B.is_recurring OR (T.is_recurring IS NULL AND B.is_recurring IS NULL))
  AND (T.user_tracking_allowed = B.user_tracking_allowed OR (T.user_tracking_allowed IS NULL AND B.user_tracking_allowed IS NULL))

  AND (T.seller_id = B.seller_id OR (T.seller_id IS NULL AND B.seller_id IS NULL))

  -- child_product_id with same normalization as backup
  AND (
    COALESCE(NULLIF(TRIM(T.child_product_id), ''), CAST(T.product_id AS STRING)) =
    COALESCE(NULLIF(TRIM(B.child_product_id), ''), CAST(B.product_id AS STRING))
  )

  -- finally, symbiosys_ts
  AND (T.symbiosys_ts = B.symbiosys_ts OR (T.symbiosys_ts IS NULL AND B.symbiosys_ts IS NULL))
    AND T.order_ts >= TIMESTAMP('2025-07-27 00:00:00')
    AND T.order_ts < TIMESTAMP('2025-07-30 00:00:00')

WHEN MATCHED THEN
  DELETE;