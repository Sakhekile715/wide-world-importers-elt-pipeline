WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'sales_customers_archive',
        'customer_id, valid_from, valid_to',
        '_batch_id DESC'
    ) }}
)

SELECT
    customer_id,
    customer_name,
    bill_to_customer_id,
    customer_category_id,
    buying_group_id,
    primary_contact_person_id,
    alternate_contact_person_id,
    delivery_method_id,
    delivery_city_id,
    postal_city_id,
    credit_limit,
    account_opened_date,
    standard_discount_percentage,
    is_statement_sent,
    is_on_credit_hold,
    payment_days,
    valid_from,
    valid_to
FROM deduped
