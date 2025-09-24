with
    raw as (
        select * 
        from {{ ref('productsubcategory') }}
    )

    , renamed as (
        select
            cast(ProductSubcategoryID as int) as product_subcategory_id
            , cast(ProductCategoryID as int) as product_category_id
            , Name as subcategory_description
            , rowguid
            , ModifiedDate as updated_at
            , extraction_date as extracted_at
        from raw    
    )

select *
from renamed
