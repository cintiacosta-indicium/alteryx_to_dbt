with
    raw as (
        select * 
        from {{ ref('stateprovince') }}
    )

    , renamed as (
        select
            cast(StateProvinceID as int) as state_province_id
            , StateProvinceCode as state_province_code
            , CountryRegionCode as country_region_code
            , IsOnlyStateProvinceFlag as is_only_state_province_flag
            , Name as territory_name
            , cast(TerritoryID as int) as territory_id
            , cast(ModifiedDate as date) as updated_at
            , cast(extraction_date as timestamp) as extracted_at
        from raw    
    )

select * 
from renamed
where territory_name not like '%?%'
