with
    raw as (
        select * 
        from {{ ref('productcategory') }}
    )

    , renamed as (
        select
            cast(ProductCategoryID as int) as product_category_id
            , Name as category_description
            , rowguid
            , cast(ModifiedDate as date) as updated_at
            , cast(extraction_date as timestamp) as extracted_at
        from raw    
    )

select *
from renamed
