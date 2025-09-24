with
    purchase_order_header as (
        select 
            purchase_order_id
            , revision_number
            , purchase_order_status
            , employee_id
            , vendor_id
            , ship_method_id
            , subtotal
            , tax_amt
            , freight
            , total_due
            , order_date
            , ship_date
            , updated_at
            , extracted_at
        from {{ ref("stg_purchase_order_header") }}
    )

    , purchase_order_detail as (
        select 
            purchase_order_id
            , purchase_order_detail_id
            , order_qty
            , product_id
            , unit_price
            , line_total
            , received_qty
            , rejected_qty
            , stocked_qty
            , due_date
            , updated_at
            , extracted_at 
        from {{ ref("stg_purchase_order_detail") }}
    )

    , ship_method as (
        select 
            {{ dbt_utils.default__generate_surrogate_key(
                ['ship_method_id']
            ) }} as ship_method_sk
            , ship_method_id
            , ship_company_name
            , ship_base_charge
            , ship_charge_per_pound
            , loaded_at
        from {{ ref("stg_ship_method") }}
    )

    , joined_purchase as (
        select
            {{ dbt_utils.default__generate_surrogate_key(
                ['purchase_order_header.purchase_order_id']
            ) }} as purchase_order_sk
            , purchase_order_header.purchase_order_id
            , purchase_order_header.revision_number
            , purchase_order_header.purchase_order_status
            , purchase_order_header.employee_id
            , purchase_order_header.vendor_id
            , purchase_order_header.ship_method_id
            , purchase_order_header.subtotal
            , purchase_order_header.tax_amt
            , purchase_order_header.freight
            , purchase_order_header.total_due
            , purchase_order_header.order_date
            , purchase_order_header.ship_date
            , purchase_order_header.updated_at
            , purchase_order_header.extracted_at
            , purchase_order_detail.purchase_order_detail_id
            , purchase_order_detail.order_qty
            , purchase_order_detail.product_id
            , purchase_order_detail.unit_price
            , purchase_order_detail.line_total
            , purchase_order_detail.received_qty
            , purchase_order_detail.rejected_qty
            , purchase_order_detail.stocked_qty
            , purchase_order_detail.due_date
            , ship_method.ship_method_sk
            , ship_method.ship_company_name
            , ship_method.ship_base_charge
            , ship_method.ship_charge_per_pound
            , ship_method.loaded_at
        from purchase_order_header
        left join purchase_order_detail on
            purchase_order_header.purchase_order_id = purchase_order_detail.purchase_order_id
        left join ship_method on
            purchase_order_header.ship_method_id = ship_method.ship_method_id
    )

select *
from joined_purchase
