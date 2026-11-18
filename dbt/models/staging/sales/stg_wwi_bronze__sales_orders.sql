WITH deduped AS (
    {{ dedup_latest_batch(
        'wwi_bronze',
        'sales_orders',
        'order_id',
        '_batch_id DESC, last_edited_when DESC'
    ) }}
)

SELECT
    order_id,
    customer_id,
    salesperson_person_id,
    picked_by_person_id,
    contact_person_id,
    backorder_order_id,
    order_date,
    expected_delivery_date,
    customer_purchase_order_number,
    is_undersupply_backordered,
    picking_completed_when
FROM deduped
