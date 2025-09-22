with
    raw as (
        select * 
        from {{ ref('purchaseorderheader') }}
    )

    , renamed as (
        select
            cast(PurchaseOrderID as int) as purchase_order_id
            , cast(RevisionNumber as int) as revision_number
            , cast(Status as int) as purchase_order_status
            , cast(EmployeeID as int) as employee_id
            , cast(VendorID as int) as vendor_id
            , cast(ShipMethodID as int) as ship_method_id
            , cast(SubTotal as float) as subtotal
            , cast(TaxAmt as float) as tax_amt
            , cast(Freight as float) as freight
            , cast(TotalDue as float) as total_due
            , cast(OrderDate as date) as order_date
            , cast(ShipDate as date) as ship_date
            , cast(ModifiedDate as date) as updated_at
            , cast(extraction_date as timestamp) as extracted_at
        from raw    
    )

select *
from renamed
