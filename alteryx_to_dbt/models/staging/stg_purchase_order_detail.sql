with
    raw as (
        select * 
        from {{ ref('purchaseorderdetail') }}
    )

    , renamed as (
        select
            cast(PurchaseOrderID as int) as purchase_order_id
            , cast(PurchaseOrderDetailID as int) as purchase_order_detail_id
            , cast(OrderQty as int) as order_qty
            , cast(ProductID as int) as product_id
            , cast(UnitPrice as float) as unit_price
            , cast(LineTotal as float) as line_total
            , cast(ReceivedQty as int) as received_qty
            , cast(RejectedQty as int) as rejected_qty
            , cast(StockedQty as int) as stocked_qty
            , cast(DueDate as date) as due_date
            , cast(ModifiedDate as date) as updated_at
            , cast(extraction_date as timestamp) as extracted_at 
        from raw    
    )

select *
from renamed
