with
    raw as (
        select * 
        from {{ ref('salesperson') }}
    )

    , renamed as (
        select
            cast(BusinessEntityID as int) as business_entity_id
            , cast(TerritoryID as int) as territory_id
            , cast(SalesQuota as float) as sales_quota
            , cast(Bonus as float) as bonus
            , cast(CommissionPct as float) as commission_pct
            , cast(SalesYTD as float) as sales_ytd
            , cast(SalesLastYear as float) as sales_last_year
            , rowguid as row_guid
            , cast(ModifiedDate as date) as updated_at
            , cast(extraction_date as date) as extracted_at
        from raw
    )

select * 
from renamed
where territory_id is not null
