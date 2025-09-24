with
    orders as (
        select 
            purchase_order_detail_id
            , purchase_order_id
            , ship_method_sk
            , due_date
            , order_date
            , ship_date
            , purchase_order_status
            , received_qty
            , rejected_qty
            , stocked_qty
            , order_qty
            , unit_price
            , line_total
            , tax_amt
            , freight
            , updated_at
            , extracted_at
            , employee_id
            , vendor_id
            , product_id
        from {{ ref("int_purchasing_orders") }}
    )

    , products as (
        select 
            product_sk
            , product_id
        from {{ ref("int_products") }}
    )

    , suppliers as (
        select 
            supplier_sk
            , business_entity_id
        from {{ ref("int_suppliers") }}
    )

    , employees as (
        select 
            employee_sk
            , business_entity_id
        from {{ ref("int_employees") }}
    )

    , purchasing_orders as (
        select
            {{ dbt_utils.default__generate_surrogate_key(
                ['orders.purchase_order_detail_id']
            ) }} as purchasing_order_sk
            , orders.purchase_order_detail_id
            , orders.purchase_order_id
            , products.product_sk
            , orders.ship_method_sk
            , suppliers.supplier_sk
            , employees.employee_sk
            , orders.due_date
            , orders.order_date
            , orders.ship_date
            , datediff(
                day, orders.order_date, orders.due_date
            ) as days_expected_to_receive
            , datediff(
                day, orders.ship_date, orders.due_date
            ) as estimated_shipment
            , orders.purchase_order_status
            , orders.received_qty
            , orders.rejected_qty
            , orders.stocked_qty
            , orders.order_qty
            , orders.unit_price
            , orders.line_total
            , orders.tax_amt
            , orders.freight
            , orders.line_total + orders.tax_amt + orders.freight 
            as total_due_per_product
            , orders.updated_at
            , orders.extracted_at
        from orders
        left join products on
            orders.product_id = products.product_id
        left join suppliers on
            orders.vendor_id = suppliers.business_entity_id        
        left join employees on
            orders.employee_id = employees.business_entity_id        
    )

select *
from purchasing_orders
