with 
    supplier as (
        select
            business_entity_id
            , account_number
            , vendor_company_name
            , credit_rating
            , preferred_vendor_status
            , activeflag
            , purchasing_web_service_url
            , updated_at
            , extracted_at
        from {{ ref("stg_vendor") }}
    )

    , business_entity as (
        select
            business_entity_id
            , rowguid
            , updated_at
            , extracted_at
        from {{ ref("stg_business_entity") }}
    )

    , joined_supplier as (
        select
            {{ dbt_utils.default__generate_surrogate_key(
                ['supplier.business_entity_id']
            ) }} as supplier_sk
            , supplier.business_entity_id
            , supplier.account_number
            , supplier.vendor_company_name
            , supplier.credit_rating
            , supplier.preferred_vendor_status
            , supplier.activeflag
            , supplier.purchasing_web_service_url
            , supplier.updated_at
            , supplier.extracted_at
            , business_entity.rowguid
        from supplier
        left join business_entity on
            supplier.business_entity_id = business_entity.business_entity_id
    )

select *
from joined_supplier
