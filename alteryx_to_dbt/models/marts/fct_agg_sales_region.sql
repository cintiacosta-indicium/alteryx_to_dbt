with 
    sales_territory as (
        select 
            territory_id
            , territory_name
            , country_region_code
            , group_name
            , sales_ytd
            , sales_last_year
            , cost_ytd
            , cost_last_year
            , row_guid
            , updated_at
            , extracted_at
        from {{ ref("stg_sales_territory") }}
    )

    , sales_person as (
        select 
            business_entity_id
            , territory_id
            , sales_quota
            , bonus
            , commission_pct
            , sales_ytd
            , sales_last_year
            , row_guid
            , updated_at
            , extracted_at
        from {{ ref("stg_sales_person") }}
    )

    , country_region as (
        select 
            country_region_code
            , country_region_name
            , updated_at
            , extracted_at
        from {{ ref("stg_country_region") }}
    )

    , state_province as (
        select 
            state_province_id
            , state_province_code
            , country_region_code
            , is_only_state_province_flag
            , territory_name
            , territory_id
            , updated_at
            , extracted_at
        from {{ ref("stg_state_province") }}
    )

    , joined_sales as (
        select 
            sales_person.territory_id
            , sales_territory.territory_name
            , sales_territory.country_region_code
            , sales_territory.group_name
            , sales_person.sales_ytd
            , sales_person.sales_last_year
            , sales_territory.cost_ytd
            , sales_territory.cost_last_year
            , sales_territory.row_guid
            , sales_territory.updated_at
            , sales_territory.extracted_at
            , sales_person.business_entity_id
            , sales_person.sales_quota
            , sales_person.bonus
            , sales_person.commission_pct
        from sales_person
        left join sales_territory on
            sales_person.territory_id = sales_territory.territory_id
    )

    , joined_region as (
        select 
            country_region.country_region_code
            , country_region.country_region_name
            , country_region.updated_at
            , country_region.extracted_at
            , state_province.state_province_id
            , state_province.state_province_code
            , state_province.territory_name
            , state_province.territory_id
        from country_region
        left join state_province on
            country_region.country_region_code = state_province.country_region_code
    )

    , joined_sales_region as (
        select 
            joined_sales.territory_id
            , joined_region.state_province_id
            , joined_sales.business_entity_id
            , joined_region.state_province_code
            , joined_region.country_region_code
            , joined_sales.group_name
            , joined_region.country_region_name
            , joined_region.territory_name
            , joined_sales.sales_ytd
            , joined_sales.sales_last_year
            , joined_sales.cost_ytd
            , joined_sales.cost_last_year
            , joined_sales.sales_quota
            , joined_sales.bonus
            , joined_sales.commission_pct
            , joined_region.extracted_at
            , joined_region.updated_at
        from joined_sales
        left join joined_region on
            joined_sales.territory_id = joined_region.territory_id
    )

    , agg_sales_region as (
        select 
            territory_id
            , state_province_id
            , business_entity_id
            , country_region_code
            , state_province_code
            , group_name
            , country_region_name
            , territory_name
            , sum(sales_ytd) as total_sales_ytd
            , sum(sales_last_year) as total_sales_last_year
            , sum(bonus) as total_bonus
            , max(updated_at) as updated_at
            , max(extracted_at) as extracted_at
        from joined_sales_region
        group by 
            territory_id
            , territory_name
            , group_name
            , country_region_name
            , country_region_code
            , state_province_id
            , state_province_code
            , business_entity_id
    )

select *
-- Validate the table with the original one on Alteryx by summing numeric values
    -- sum(total_sales_ytd) as total_sales_ytd
    -- , sum(total_sales_last_year) as total_sales_last_year
    -- , sum(total_bonus) as total_bonus
from agg_sales_region
