with
    raw as (
        select * 
        from {{ ref('shipmethod') }}
    )

    , renamed as (
        select
            cast(ShipMethodID as int) as ship_method_id
            , Name as ship_company_name
            , cast(ShipBase as float) as ship_base_charge
            , cast(ShipRate as float) as ship_charge_per_pound
            , rowguid
            , cast(ModifiedDate as date) as updated_at
            , cast(extraction_date as timestamp) as extracted_at
            , '{{ dbt_utils.pretty_time(format='%Y-%m-%d %H:%M:%S') }}' as loaded_at
        from raw    
    )

select * 
from renamed
