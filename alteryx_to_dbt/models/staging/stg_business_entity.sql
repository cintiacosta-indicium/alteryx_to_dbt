with
    raw as (
        select * 
        from {{ ref('businessentity') }}
    )

    , renamed as (
        select
            cast(BusinessEntityID as int) as business_entity_id
            , rowguid
            , cast(ModifiedDate as date) as updated_at
            , cast(extraction_date as timestamp) as extracted_at
        from raw    
    )

select * 
from renamed
