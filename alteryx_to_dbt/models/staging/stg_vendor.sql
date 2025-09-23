with
    raw as (
        select * 
        from {{ ref('vendor') }}
    )

    , renamed as (
        select
            cast(BusinessEntityID as int) as business_entity_id
            , AccountNumber as account_number
            , Name as vendor_company_name
            , cast(CreditRating as int) as credit_rating
            , cast(PreferredVendorStatus as boolean) as preferred_vendor_status
            , cast(ActiveFlag as boolean) as activeflag
            , cast(PurchasingWebServiceURL as string) as purchasing_web_service_url
            , cast(ModifiedDate as date) as updated_at
            , cast(extraction_date as timestamp) as extracted_at
        from raw    
    )

select * 
from renamed
