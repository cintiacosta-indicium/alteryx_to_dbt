with
    raw as (
        select * 
        from {{ ref('countryregion') }}
    )

    , renamed as (
        select
            CountryRegionCode as country_region_code
            , Name as country_region_name
            , cast(ModifiedDate as date) as updated_at
            , cast(extraction_date as timestamp) as extracted_at
        from raw    
    )

select * 
from renamed
