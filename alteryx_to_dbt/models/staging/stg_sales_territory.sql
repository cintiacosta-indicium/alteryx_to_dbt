with
    raw as (
        select * 
        from {{ ref('salesterritory') }}
    )

    , renamed as (
        select
            cast(TerritoryID as int) as territory_id
            , Name as territory_name
            , CountryRegionCode as country_region_code
            , GroupName as group_name
            , cast(SalesYTD as float) as sales_ytd
            , cast(SalesLastYear as float) as sales_last_year
            , cast(CostYTD as float) as cost_ytd
            , cast(CostLastYear as float) as cost_last_year
            , rowguid as row_guid
            , cast(ModifiedDate as date) as updated_at
            , cast(extraction_date as date) as extracted_at
        from raw    
    )

select * 
from renamed
